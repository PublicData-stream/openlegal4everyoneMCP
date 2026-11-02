"""Root /tmp fixtures and harmless Python children only; no Docker/ZFS daemon.

Synthetic command results do not establish native Docker/PG/HDD acceptance.
"""
import copy
import gzip
import io
import importlib.util
import os
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
import command_process
spec = importlib.util.spec_from_file_location('diagnostic_daemon', BASE / 'diagnostic_daemon.py')
d = importlib.util.module_from_spec(spec); spec.loader.exec_module(d)


def oci_fixture(*, native_platform=None, duplicate_native=False, config_os='linux', config_arch='amd64',
                layer_media=None,legacy_tags=None,config_padding=''):
    """Tiny OCI metadata fixture, not a runnable/accepted Docker image."""
    stream=io.BytesIO()
    with tarfile.open(fileobj=stream,mode='w') as layer:
        item=tarfile.TarInfo('synthetic-fixture');payload=b'fictional test bytes'
        item.size=len(payload);layer.addfile(item,io.BytesIO(payload))
    uncompressed=stream.getvalue();compressed=gzip.compress(uncompressed,mtime=0)
    diff='sha256:'+d.sha(uncompressed);layer_id='sha256:'+d.sha(compressed)
    config={'architecture':config_arch,'os':config_os,'config':{'Env':['NEVER_OUTPUT_FIXTURE_SECRET=value'+config_padding]},
            'rootfs':{'type':'layers','diff_ids':[diff]}}
    config_raw=d.canonical(config);config_id='sha256:'+d.sha(config_raw)
    manifest={'schemaVersion':2,'mediaType':d.MANIFEST_MEDIA,
        'config':{'mediaType':d.CONFIG_MEDIA,'digest':config_id,'size':len(config_raw)},
        'layers':[{'mediaType':layer_media or d.LAYER_MEDIA,'digest':layer_id,'size':len(compressed)}]}
    manifest_raw=d.canonical(manifest);manifest_id='sha256:'+d.sha(manifest_raw)
    descriptor={'mediaType':d.MANIFEST_MEDIA,'digest':manifest_id,'size':len(manifest_raw),
                'platform':native_platform or {'os':'linux','architecture':'amd64'}}
    official={'schemaVersion':2,'mediaType':d.INDEX_MEDIA,'manifests':[descriptor]}
    if duplicate_native:official['manifests'].append(copy.deepcopy(descriptor))
    official_raw=d.canonical(official);official_id='sha256:'+d.sha(official_raw)
    index={'schemaVersion':2,'mediaType':d.INDEX_MEDIA,'manifests':[
        {'mediaType':d.INDEX_MEDIA,'digest':official_id,'size':len(official_raw)}]}
    path=lambda value:'blobs/sha256/'+value.split(':')[1]
    files={'oci-layout':d.canonical({'imageLayoutVersion':'1.0.0'}),'index.json':d.canonical(index),
        'manifest.json':d.canonical([{'Config':path(config_id),'Layers':[path(layer_id)],'RepoTags':legacy_tags}]),
        path(official_id):official_raw,path(manifest_id):manifest_raw,path(config_id):config_raw,path(layer_id):compressed}
    return {'files':files,'official_image_id':official_id,'platform_manifest_digest':manifest_id,
            'image_config_digest':config_id,'diff_ids':[diff],'layer_path':path(layer_id),'config_path':path(config_id)}


def tar_fixture(files,extras=(),*,pax_headers=None):
    out=io.BytesIO()
    with tarfile.open(fileobj=out,mode='w',format=tarfile.PAX_FORMAT if pax_headers else tarfile.USTAR_FORMAT,pax_headers=pax_headers) as archive:
        for directory in ('blobs','blobs/sha256'):
            item=tarfile.TarInfo(directory);item.type=tarfile.DIRTYPE;archive.addfile(item)
        for name,raw in files.items():
            item=tarfile.TarInfo(name);item.size=len(raw);archive.addfile(item,io.BytesIO(raw))
        for item,raw in extras:archive.addfile(item,io.BytesIO(raw) if raw is not None else None)
    return out.getvalue()


def inventory(operation=None):
    operation = operation or str(uuid.uuid4());fixture=oci_fixture()
    return {'schema_version': 1, 'kind': 'openlegal_hdd_private_daemon_inventory',
        'operation_id': operation, 'host_boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
        'total_deadline_secs': 30, 'image_archive_max_bytes': 1024**2,
        'official_reference':'postgres@'+fixture['official_image_id'],
        **{k:fixture[k] for k in ('official_image_id','platform_manifest_digest','image_config_digest')},
        'binaries': {'python': {'path': str(Path(sys.executable).resolve()),
                               'sha256': d.sha(Path(sys.executable).resolve().read_bytes())},
                     'docker': {'path': '/usr/bin/docker', 'sha256': 'a' * 64},
                     'dockerd': {'path': '/usr/bin/dockerd', 'sha256': 'b' * 64},
                     **{n: {'path': '/usr/bin/' + n, 'sha256': digest} for n, digest in d.RUNTIME_PINS.items()}},
        'source_graph': {'hdd_benchmark.py': d.HDD_SOURCE, 'interface.py': 'a' * 64,
                         'command_process.py': 'b' * 64},
        'hdd_inventory': {'path': '/tmp/synthetic/input.json', 'sha256': 'c' * 64},
        'hdd_approval': {'path': '/tmp/synthetic/approval.json', 'sha256': 'd' * 64}}


def authority(value):
    return {'schema_version': 1, 'kind': 'openlegal_hdd_private_daemon_authority',
        'approved': True, 'operation_id': value['operation_id'], 'inventory_sha256': '1' * 64,
        'source_sha256': '2' * 64, 'plan_sha256': d.sha(d.canonical(d.plan(value))),
        'private_daemon_permitted': True, 'default_daemon_read_only': True,
        'approved_hdd_cli_only': True, 'production_changes_permitted': False,
        'registry_calls_permitted': False, 'automatic_retry': False, 'valid_until': time.time() + 600}


def result(**changes):
    values = dict(stdout=b'', stderr=b'', actual_exit_code=0, expired=False,
        output_limit_exceeded=False, owned_processes_closed=True, unexpected_descendants=False,
        elapsed_seconds=.01, started=True, interrupted=False)
    values.update(changes); return types.SimpleNamespace(**values)


class SchemaTests(unittest.TestCase):
    def test_runtime_profile_is_closed_and_all_helper_hashes_are_required(self):
        value=inventory();self.assertEqual(len(value['binaries']),6)
        for name in d.RUNTIME_PINS:
            for change in ('missing','hash','path'):
                bad=copy.deepcopy(value)
                if change=='missing':del bad['binaries'][name]
                elif change=='hash':bad['binaries'][name]['sha256']='0'*64
                else:bad['binaries'][name]['path']='/usr/local/bin/'+name
                with self.subTest(name=name,change=change),self.assertRaises(d.Refused):d.validate_inventory(bad)

    def test_runtime_socket_hash_width_and_non_moby_namespaces_are_bound_to_plan(self):
        first=inventory();second=inventory()
        self.assertNotEqual(d.runtime_socket_root(first),d.runtime_socket_root(second))
        root=d.runtime_socket_root(first)
        self.assertLessEqual(len(os.fsencode(root/('f'*64))),106)
        self.assertGreater(len(os.fsencode(d.root_path(first)/'sh'/('f'*64))),106)
        namespaces=d.runtime_namespaces(first)
        self.assertEqual(len(set(namespaces)),2)
        self.assertFalse(any('moby' in n for n in namespaces))
        plan=d.plan(first)
        self.assertEqual(plan['runtime_socket_root'],str(root))
        self.assertEqual(plan['runtime_config_sha256'],d.sha(d.runtime_config(first)))
        args=d.daemon_args(first)
        self.assertIn('--containerd='+str(d.root_path(first)/'c.sock'),args)
        self.assertIn('--containerd-namespace='+namespaces[0],args)
        self.assertIn('--containerd-plugins-namespace='+namespaces[1],args)

    def test_literal_runtime_config_disables_host_discovery_and_external_services(self):
        value=inventory();config=d.tomllib.loads(d.runtime_config(value).decode())
        self.assertEqual(config['imports'],[]);self.assertEqual(config['version'],4)
        for name in ('root','state','temp'):self.assertTrue(config[name].startswith(str(d.root_path(value))+'/'))
        self.assertEqual(set(config['disabled_plugins']),set(d.DISABLED_PLUGINS))
        self.assertIn('io.containerd.nri.v1.nri',config['disabled_plugins'])
        self.assertIn('io.containerd.image-verifier.v1.bindir',config['disabled_plugins'])
        self.assertEqual(config['plugins']['io.containerd.shim.v1.manager']['socket_dir'],str(d.runtime_socket_root(value)))
        self.assertEqual(config['cgroup']['path'],'')
        self.assertNotIn('proxy_plugins',config)
        env=d.payload_env(value)
        self.assertEqual(env['PATH'],'/usr/bin');self.assertEqual(env['TMPDIR'],config['temp'])
        self.assertEqual(env['DOCKER_TMPDIR'],config['temp']);self.assertNotIn('HOME',env)

    def test_import_is_inert(self):
        with patch('os.mkdir', side_effect=AssertionError('no write')), \
             patch('subprocess.Popen', side_effect=AssertionError('no actor')):
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)

    def test_closed_inventory_and_exact_approved_plan(self):
        value = inventory(); d.validate_inventory(value)
        auth = authority(value); d.validate_authority(auth, value, '1' * 64, '2' * 64)
        for change in ({'official_reference': 'postgres:18'}, {'official_image_id':'sha256:'+'0'*64},
                       {'platform_manifest_digest':'https://not-an-image.invalid'}, {'image_config_digest':None}, {'image_archive_max_bytes': 0},
                       {'total_deadline_secs': 1801}, {'operation_id': '../production'},
                       {'source_graph': dict(value['source_graph'], **{'extra.py': 'a' * 64})},
                       {'source_graph': dict(value['source_graph'], **{'hdd_benchmark.py': '0' * 64})},
                       {'command': ['systemctl', 'restart', 'docker']}):
            with self.subTest(change=change), self.assertRaises(d.Refused):
                d.validate_inventory(dict(value, **change))
        for change in ({'approved': False}, {'production_changes_permitted': True},
                       {'registry_calls_permitted': True}, {'source_sha256': '3' * 64},
                       {'valid_until': float('inf')}, {'valid_until': time.time() - 1}):
            with self.subTest(change=change), self.assertRaises(d.Refused):
                d.validate_authority(dict(auth, **change), value, '1' * 64, '2' * 64)

    def test_controller_refuses_non_main_or_multiple_os_threads_before_effects(self):
        p=types.SimpleNamespace(value={'total_deadline_secs':3});errors=[]
        before=signal.getsignal(signal.SIGCHLD);controller=d.Controller(p)
        def call():
            try:controller.setup()
            except BaseException as error:errors.append(error)
        thread=threading.Thread(target=call);thread.start();thread.join()
        self.assertEqual(len(errors),1);self.assertIsInstance(errors[0],d.Refused)
        self.assertIn('dedicated_main_thread',str(errors[0]));self.assertEqual(signal.getsignal(signal.SIGCHLD),before)
        ready=threading.Event();stop=threading.Event()
        thread=threading.Thread(target=lambda:(ready.set(),stop.wait()))
        thread.start();ready.wait()
        try:
            with self.assertRaisesRegex(d.Refused,'dedicated_main_thread'):d.Controller(p).setup()
        finally:stop.set();thread.join()
        self.assertEqual(signal.getsignal(signal.SIGCHLD),before)

    def test_controller_refuses_preexisting_children_before_signal_or_filesystem_effects(self):
        child=subprocess.Popen([sys.executable,'-I','-B','-c','import time;time.sleep(5)'])
        before=signal.getsignal(signal.SIGCHLD)
        try:
            with self.assertRaisesRegex(d.Refused,'preexisting_children'):
                d.Controller(types.SimpleNamespace(value={'total_deadline_secs':3})).setup()
            self.assertEqual(signal.getsignal(signal.SIGCHLD),before)
        finally:child.terminate();child.wait(timeout=3)

    def test_unsupported_changed_sigchld_context_refuses_spawn_and_wait(self):
        controller=d.Controller(types.SimpleNamespace(value={'total_deadline_secs':3}))
        controller._acquire_sigchld()
        try:
            signal.signal(signal.SIGCHLD,signal.SIG_IGN)
            with patch.object(d.subprocess,'Popen',side_effect=AssertionError('no spawn')):
                with self.assertRaisesRegex(d.Refused,'sigchld_not_owned'):controller.spawn('default-inspect')
            with self.assertRaisesRegex(d.Refused,'sigchld_not_owned'):controller.wait('default-inspect')
        finally:
            signal.signal(signal.SIGCHLD,signal.SIG_DFL);controller._restore_sigchld()

    def test_daemon_routes_every_artifact_under_fixed_tmp_root_and_disables_network_changes(self):
        value = inventory(); args = d.daemon_args(value); root = str(d.root_path(value))
        for flag in ('--bridge=none', '--iptables=false', '--ip6tables=false', '--ip-forward=false',
                     '--ip-masq=false', '--userland-proxy=false', '--storage-driver=vfs'):
            self.assertIn(flag, args)
        for arg in args:
            if arg.startswith(('--config-file=', '--data-root=', '--exec-root=', '--pidfile=')):
                self.assertTrue(arg.split('=', 1)[1].startswith(root + '/'))
        self.assertEqual([a for a in args if a.startswith('--host=')], ['--host=unix://' + root + '/docker.sock'])
        self.assertFalse(any('tcp://' in a or 'systemctl' in a for a in args))

    def test_strict_actual_result_never_accepts_expiry_descendant_or_unknown(self):
        d.result_ok(result())
        for change in ({'expired': True}, {'interrupted': True}, {'unexpected_descendants': True},
                       {'owned_processes_closed': False}, {'actual_exit_code': None},
                       {'actual_exit_code': 1}, {'actual_exit_code': False},
                       {'output_limit_exceeded': True}, {'started': False}, {'started': 1},
                       {'stdout': b'x' * d.LIMIT, 'stderr': b'x'}):
            with self.subTest(change=change), self.assertRaises(d.Refused): d.result_ok(result(**change))

    def test_duplicate_nonfinite_and_deep_json_refuse(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'[' * 30 + b'0' + b']' * 30):
            with self.assertRaises(d.Refused): d.decode(raw)

    def test_hdd_route_dependencies_native_image_domain_are_exactly_coupled(self):
        value = inventory(); root = d.root_path(value)
        image_id = value['image_config_digest']
        hdd = {'host_boot_id': value['host_boot_id'],
            'docker': {'endpoint': 'unix://' + str(root / 'docker.sock'), 'root_dir': str(root / 'data')},
            'binaries': {'docker': value['binaries']['docker']},
            'process_sources': {k: value['source_graph'][k] for k in ('interface.py', 'command_process.py')},
            'native_pg_image': {'image_id': image_id, 'reference': image_id, 'os': 'linux',
                'architecture': {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(d.platform.machine())}}
        d.validate_hdd_binding(value, hdd)
        for key in ('route', 'boot', 'binary', 'dependency', 'reference'):
            bad = copy.deepcopy(hdd)
            if key == 'route': bad['docker']['endpoint'] = 'unix:///var/run/docker.sock'
            elif key == 'boot': bad['host_boot_id'] = str(uuid.uuid4())
            elif key == 'binary': bad['binaries']['docker']['sha256'] = 'f' * 64
            elif key == 'dependency': bad['process_sources']['command_process.py'] = 'f' * 64
            else: bad['native_pg_image']['reference'] = value['official_reference']
            with self.subTest(key=key), self.assertRaises(d.Refused): d.validate_hdd_binding(value, bad)


@unittest.skipUnless(os.geteuid() == 0, 'root ownership gate')
class FilesystemTests(unittest.TestCase):
    def setUp(self):
        self.fixture=oci_fixture();self.value = inventory(); self.root = d.root_path(self.value); self.root.mkdir(mode=0o700)
        self.addCleanup(lambda: __import__('shutil').rmtree(self.root))
        self.e = d.Evidence(self.root); self.addCleanup(self.e.close)
        for name in ('client', 'data', 'exec', *d.RUNTIME_DIRS): (self.root / name).mkdir(mode=0o700)
        short = d.runtime_socket_root(self.value); short.mkdir(mode=0o700)
        self.addCleanup(lambda: __import__('shutil').rmtree(short))
        se = d.Evidence(short)
        try:se.save('owner.json', {'operation_id': self.value['operation_id'], 'root': str(self.root),
            'root_identity': [self.e.identity.st_dev, self.e.identity.st_ino]})
        finally:se.close()
        self.e.save('layout.json', {n: [(s := (self.root / n).stat()).st_dev, s.st_ino]
            for n in ('client', 'data', 'exec', *d.RUNTIME_DIRS, str(short))})
        self.e.save_bytes('daemon.json', b'{}\n')
        self.e.save_bytes('containerd.toml', d.runtime_config(self.value))
        self.p = types.SimpleNamespace(value=self.value, hv={'native_pg_image': {
            'image_id':self.value['image_config_digest'],'reference':self.value['image_config_digest'],
            'os': 'linux', 'architecture': 'amd64'}},
            args=types.SimpleNamespace(inventory_sha256='1' * 64, source_sha256='2' * 64),
            process=types.SimpleNamespace(execute=self.execute, _children=lambda _: set()))
        self.calls = []
        self.seal = patch.object(d, 'seal', lambda *_: None); self.seal.start(); self.addCleanup(self.seal.stop)
        # Existing Docker results are synthetic. Runtime pin checks receive their
        # own real-FD/negative tests below; no test starts an installed runtime.
        runtimepins=patch.object(d,'check_runtime_binaries',lambda *_:None)
        runtimepins.start();self.addCleanup(runtimepins.stop)

    def execute(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        identity=self.value['official_image_id'] if 'unix:///var/run/docker.sock' in argv else self.value['image_config_digest']
        image = dict(Id=identity,Os='linux',Architecture='amd64',Size=512,RootFS=self.fixture['diff_ids'])
        return result(stdout=d.canonical(image))

    def write_archive(self,fixture=None,*,files=None,extras=()):
        fixture=fixture or self.fixture
        for key in ('official_image_id','platform_manifest_digest','image_config_digest'):
            self.value[key]=fixture[key]
        self.value['official_reference']='postgres@'+fixture['official_image_id']
        self.p.hv['native_pg_image']['reference']=self.p.hv['native_pg_image']['image_id']=fixture['image_config_digest']
        path=self.root/'image.tar';path.write_bytes(tar_fixture(files if files is not None else fixture['files'],extras));path.chmod(0o600)
        note={'Id':fixture['official_image_id'],'Os':'linux','Architecture':'amd64','Size':512,'RootFS':fixture['diff_ids']}
        if not (self.root/'default-image-binding.json').exists():
            self.e.save('default-image-binding.json',{'official_reference':self.value['official_reference'],
                'official_image_id':self.value['official_image_id'],'inspections':[note,note]})
        return path

    def private_socket(self):
        for name in ('c.sock','c.ttrpc'):
            server=socket.socket(socket.AF_UNIX);server.bind(str(self.root/name));server.listen(1)
            self.addCleanup(server.close)
        metadata=self.root/'cd/io.containerd.metadata.v1.bolt/meta.db'
        metadata.parent.mkdir();metadata.write_bytes(b'synthetic metadata, not containerd')
        fd=os.open(metadata,os.O_RDONLY);self.addCleanup(os.close,fd)
        self.e.save('containerd-native-owner.json',{'witness':d.witness(os.getpid()),
            'sockets':{n:d.owned_socket(os.getpid(),self.root/n) for n in ('c.sock','c.ttrpc')},
            'metadata_db':d.runtime_storage(os.getpid(),self.e)})
        sock = socket.socket(socket.AF_UNIX); sock.bind(str(self.root / 'docker.sock')); self.addCleanup(sock.close)
        node = (self.root / 'docker.sock').lstat()
        self.e.save('daemon-native-owner.json', {'witness': d.witness(os.getpid()),
            'socket': {'dev': node.st_dev, 'ino': node.st_ino}})

    def test_fixed_actions_never_emit_default_daemon_mutations_or_registry(self):
        for action in d.ACTIONS:
            commands = d.action_commands(self.p, action)
            for binary, argv in commands:
                if binary == 'docker':
                    self.assertEqual(argv[:2], ['--config', str(self.root / 'client')])
                    self.assertNotIn('pull', argv); self.assertNotIn('tag', argv)
                    if argv[3] == 'unix:///var/run/docker.sock':
                        self.assertIn(argv[4:6], (['image', 'inspect'], ['image', 'save']))
                elif binary == 'python':
                    self.assertEqual(argv[0:2], ['-I', '-B']); self.assertEqual(argv[3], 'run')
                    self.assertIn(d.HDD_SOURCE, argv)
        with self.assertRaises(d.Refused): d.action_commands(self.p, 'arbitrary-shell')

    def test_private_runtime_requires_actual_owned_socket_and_open_metadata_fd(self):
        self.private_socket();d.check_private_runtime(self.e)
        metadata=self.root/'cd/io.containerd.metadata.v1.bolt/meta.db'
        metadata.rename(metadata.with_name('old.db'));metadata.write_bytes(b'replacement')
        with self.assertRaisesRegex(d.Refused,'runtime_db_not_owned'):d.check_private_runtime(self.e)

    def test_runtime_native_namespace_directory_mismatch_refuses(self):
        self.private_socket();original=Path.stat
        target=str(Path('/proc')/str(os.getpid())/'root'/str(self.root/'cs').lstrip('/'))
        def changed(path,*a,**kw):
            if str(path)==target:return types.SimpleNamespace(st_dev=-1,st_ino=-1)
            return original(path,*a,**kw)
        with patch.object(Path,'stat',changed),self.assertRaisesRegex(d.Refused,'runtime_directory_namespace'):
            d.check_private_runtime(self.e)

    def test_runtime_socket_identity_cannot_be_an_unrelated_listening_process(self):
        # This process owns these real local fixture sockets; an unrelated PID
        # cannot claim them merely because /proc/net/unix lists the pathname.
        self.private_socket()
        child=subprocess.Popen([sys.executable,'-I','-B','-c','import time;time.sleep(5)'])
        try:
            with self.assertRaisesRegex(d.Refused,'runtime_socket_not_owned'):
                d.owned_socket(child.pid,self.root/'c.sock')
        finally:child.terminate();child.wait(timeout=3)

    def test_owned_listener_stays_exact_with_real_active_accepted_connections(self):
        path=self.root/'active.sock'
        with socket.socket(socket.AF_UNIX) as server,socket.socket(socket.AF_UNIX) as client:
            server.bind(str(path));server.listen(1)
            before=d.owned_socket(os.getpid(),path)
            client.connect(str(path));accepted,_=server.accept()
            with accepted:
                rows=[r.split() for r in Path('/proc/self/net/unix').read_text().splitlines()[1:]
                      if r.split()[-1:]==[str(path)]]
                self.assertGreaterEqual(len(rows),2)
                self.assertTrue(any(r[3:6]==['00000000','0001','03'] for r in rows))
                self.assertEqual(d.owned_socket(os.getpid(),path),before)

    def test_duplicate_real_listen_rows_same_path_are_refused_even_if_both_owned(self):
        path=self.root/'duplicate-listener.sock'
        with socket.socket(socket.AF_UNIX) as first,socket.socket(socket.AF_UNIX) as second:
            first.bind(str(path));first.listen(1);path.unlink()
            second.bind(str(path));second.listen(1)
            with self.assertRaisesRegex(d.Refused,'runtime_socket_not_owned'):
                d.owned_socket(os.getpid(),path)

    def test_nonaccepted_bound_socket_same_path_cannot_hide_path_replacement(self):
        path=self.root/'replaced-bound.sock'
        with socket.socket(socket.AF_UNIX) as first,socket.socket(socket.AF_UNIX) as second:
            first.bind(str(path));first.listen(1);path.unlink()
            second.bind(str(path))
            with self.assertRaisesRegex(d.Refused,'runtime_socket_not_owned'):
                d.owned_socket(os.getpid(),path)

    def test_short_socket_root_rebind_or_other_operation_owner_refuses(self):
        short=d.runtime_socket_root(self.value);owner=short/'owner.json'
        note=d.decode(owner.read_bytes());note['operation_id']=str(uuid.uuid4())
        owner.write_bytes(d.canonical(note));owner.chmod(0o600)
        with self.assertRaisesRegex(d.Refused,'runtime_socket_root_rebound'):d.check_layout(self.e)
        self.assertTrue(owner.exists())

    def test_short_socket_root_replacement_refuses_despite_copied_owner_bytes(self):
        d.check_layout(self.e);short=d.runtime_socket_root(self.value);old=Path(str(short)+'-old')
        short.rename(old);self.addCleanup(lambda:__import__('shutil').rmtree(old))
        short.mkdir(mode=0o700);(short/'owner.json').write_bytes((old/'owner.json').read_bytes())
        (short/'owner.json').chmod(0o600)
        with self.assertRaisesRegex(d.Refused,'layout_rebound'):d.check_layout(self.e)

    def test_held_runtime_directories_and_literal_config_cannot_be_replaced(self):
        d.check_layout(self.e)
        path=self.root/'cs';path.rename(self.root/'old-state');path.mkdir(mode=0o700)
        with self.assertRaisesRegex(d.Refused,'layout_rebound'):d.check_layout(self.e)
        path.rmdir();(self.root/'old-state').rename(path)
        config=self.root/'containerd.toml';config.write_bytes(d.runtime_config(self.value)+b'\nimports = ["/etc/containerd/config.toml"]\n')
        config.chmod(0o600)
        with self.assertRaisesRegex(d.Refused,'runtime_config_changed'):d.check_layout(self.e)

    def test_runtime_binaries_are_all_hashed_and_retained_inode_mutation_refuses(self):
        # Dedicated real /tmp executable fixtures test the pin helper itself.
        binaries={};p=types.SimpleNamespace(value={'binaries':binaries})
        for name in d.RUNTIME_PINS:
            path=self.root/name;path.write_bytes(b'harmless nonexecuted fixture '+name.encode());path.chmod(0o700)
            binaries[name]={'path':str(path),'sha256':d.sha(path.read_bytes())}
        d.hold_runtime_binaries(p)
        try:
            self.assertEqual(set(p.runtime_holds),set(d.RUNTIME_PINS));d.check_held_runtime(p)
            path=self.root/'runc';path.rename(self.root/'old-runc');path.write_bytes(b'replacement');path.chmod(0o700)
            with self.assertRaisesRegex(d.Refused,'held_runtime_binary_changed'):d.check_held_runtime(p)
        finally:d.close_runtime_binaries(p)
        binaries['runc']['sha256']='0'*64
        with self.assertRaisesRegex(d.Refused,'binary_hash'):d.hold_runtime_binaries(p)
        self.assertEqual(p.runtime_holds,{})

    def runtime_profile(self, *, change=None, warning=b''):
        self.p.value['binaries']['containerd']=self.p.value['binaries']['python']
        config=d.runtime_config(self.value)
        # Model the actual config-dump display quirk: unused default Imports is
        # printed even though the loaded file has no imports.
        config=config.replace(b'imports = []',b'imports = ["/etc/containerd/conf.d/*.toml"]')
        if change:config=change(config)
        values=[result(stdout=b'containerd containerd v2.4.2 b7a25261e029d68978bbf290df0a62fc819b3c51\n'),
                result(stdout=config,stderr=warning)]
        self.p.process.execute=lambda *a,**kw:values.pop(0)
        d.execute_action(self.p,self.e,'runtime-profile',time.monotonic()+3)

    def test_runtime_dump_known_unused_import_display_is_not_a_runtime_attestation(self):
        self.runtime_profile()
        self.assertTrue((self.root/'worker-runtime-profile-complete.json').exists())
        self.assertFalse((self.root/'containerd-native-owner.json').exists())
        self.assertFalse((self.root/'completed.json').exists())

    def test_runtime_dump_extra_import_or_global_state_or_warning_blocks_start(self):
        # Each case uses fresh evidence labels because real actions never retry.
        changes=[lambda b:b.replace(b'/etc/containerd/conf.d/*.toml',b'/etc/other.toml'),
                 lambda b:b.replace(str(self.root/'cs').encode(),b'/run/containerd')]
        for number,change in enumerate(changes):
            with self.subTest(case=number),self.assertRaises(d.Refused):self.runtime_profile(change=change)
            for path in self.root.glob('worker-runtime-profile-*'):path.unlink()
            for path in self.root.glob('runtime-profile-*'):path.unlink()
        with self.assertRaisesRegex(d.Refused,'runtime_profile_warning'):
            self.runtime_profile(warning=b'Ignoring unknown key\n')
        self.assertFalse((self.root/'worker-runtime-profile-complete.json').exists())

    def test_runtime_profile_failure_cannot_dispatch_either_daemon(self):
        c=d.Controller(self.p);events=[];c.setup=lambda:setattr(c,'evidence',self.e)
        def action(name):
            events.append(name)
            if name=='runtime-profile':raise d.Refused('synthetic native profile mismatch')
        c.action=action;c.spawn=lambda name:events.append('spawn-'+name)
        with self.assertRaisesRegex(d.Refused,'native profile mismatch'):c.run()
        self.assertFalse(any(name.startswith('spawn-') for name in events))
        self.assertFalse((self.root/'completed.json').exists())

    def test_real_two_actor_workers_native_bindings_and_ordered_actual_zero_closure(self):
        # Real harmless Python actors exercise the native bind/worker machinery.
        # They are explicitly not Docker/containerd acceptance fixtures.
        runtime_code='''import os,signal,socket,pathlib
r=pathlib.Path(ROOT)
servers=[]
for n in ('c.sock','c.ttrpc'):
 s=socket.socket(socket.AF_UNIX);s.bind(str(r/n));s.listen(1);servers.append(s)
m=r/'cd/io.containerd.metadata.v1.bolt/meta.db';m.parent.mkdir();m.write_bytes(b'synthetic')
held=open(m,'rb')
signal.signal(signal.SIGTERM,lambda *a:exit(0))
(r/'runtime-ready').write_text(str(os.getpid()))
signal.pause()
'''.replace('ROOT',repr(str(self.root)))
        daemon_code='''import os,signal,socket,pathlib
r=pathlib.Path(ROOT);s=socket.socket(socket.AF_UNIX);s.bind(str(r/'docker.sock'))
signal.signal(signal.SIGTERM,lambda *a:exit(0))
(r/'docker.pid').write_text(str(os.getpid()))
signal.pause()
'''.replace('ROOT',repr(str(self.root)))
        source=self.root/'two-actor-worker.py'
        source.write_text('import os,sys,pathlib;sys.path.insert(0,'+repr(str(BASE))+');import diagnostic_daemon as dd,command_process\n'+
            'a=sys.argv;root=pathlib.Path(__file__).resolve().parent;action=a[a.index("--worker-action")+1]\n'+
            'e=dd.Evidence(root);e.save("worker-"+action+"-ready.json",{"witness":dd.witness(os.getpid()),"action":action,"payload_inhibited":True})\n'+
            'assert os.read(int(a[a.index("--gate-fd")+1]),128).endswith((":"+action+"\\n").encode())\n'+
            'code='+repr({'containerd':runtime_code,'daemon':daemon_code})+'[action]\n'+
            'entry={"path":sys.executable,"sha256":dd.sha(pathlib.Path(sys.executable).read_bytes())}\n'+
            'fd=dd.executable(entry)\n'+
            'try:\n'+
            ' if action=="containerd":\n'+
            '  result=command_process.execute_retained_runtime([sys.executable,"-I","-B","-c",code],executable_fd=fd,stdin=None,env=dd.ENV,dispatch_deadline=float(a[a.index("--deadline")+1]),max_output=dd.LIMIT,shutdown_permission_fd=int(a[a.index("--stop-fd")+1]),owner_pidfd=int(a[a.index("--owner-fd")+1]),operation_lock_fd=int(a[a.index("--operation-lock-fd")+1]),permission_token='+repr((self.value['operation_id']+':containerd-stop\n').encode())+')\n'+
            ' else:result=command_process.execute([sys.executable,"-I","-B","-c",code],executable_fd=fd,stdin=None,env=dd.ENV,deadline=float(a[a.index("--deadline")+1]),max_output=dd.LIMIT)\n'+
            'finally:os.close(fd)\n'+
            'e.save("worker-"+action+"-0-exit.json",{"actual_exit_code":result.actual_exit_code,"owned_processes_closed":result.owned_processes_closed,"started":result.started})\n'+
            'e.save("actor-"+action+"-exit.json",{"actual_exit_code":result.actual_exit_code,"owned_processes_closed":result.owned_processes_closed})\n'+
            'dd.result_ok(result);e.save("worker-"+action+"-complete.json",{"action":action,"actual_command_exits_zero":True,"owned_processes_closed":True});e.close()\n')
        source.chmod(0o600)
        self.p.value['binaries']['containerd']=self.p.value['binaries']['python']
        self.p.value['binaries']['dockerd']=self.p.value['binaries']['python']
        self.p.process._children=command_process._children
        self.p.args=types.SimpleNamespace(inventory=self.root/'input',inventory_sha256='1'*64,
            authority=self.root/'authority',authority_sha256='3'*64,source_sha256=d.sha(source.read_bytes()))
        c=d.Controller(self.p);c.evidence=self.e;c._acquire_sigchld()
        c.lock=os.open(self.root/'operation.lock',os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
        __import__('fcntl').flock(c.lock,__import__('fcntl').LOCK_EX)
        try:
            with patch.object(d,'SELF',source),patch.object(d,'runtime_args',return_value=['-I','-B','-c',runtime_code]),\
                 patch.object(d,'daemon_args',return_value=['-I','-B','-c',daemon_code]):
                c.spawn('containerd');c.bind_runtime();d.check_private_runtime(self.e)
                c.spawn('daemon');c.bind_daemon();d.check_private_daemon(self.e)
                self.assertTrue(d.alive(c.runtime['pidfd']));self.assertTrue(d.alive(c.daemon['pidfd']))
                d.signal_owned(c.daemon,signal.SIGTERM);c.wait('daemon')
                self.assertFalse(d.alive(c.daemon['pidfd']));self.assertTrue(d.alive(c.runtime['pidfd']))
                c.permit_runtime_stop();c.wait('containerd')
                self.assertFalse(d.alive(c.runtime['pidfd']))
                self.assertEqual(command_process._children(os.getpid()),set())
        finally:
            c.contain();c._restore_sigchld()
            for row in c.workers.values():os.close(row['pidfd'])
            for row in (c.daemon,c.runtime):
                if row is not None:os.close(row['pidfd'])
            if c.runtime_permission is not None:os.close(c.runtime_permission)
            os.close(c.lock)
        for name in ('daemon','containerd'):
            note=d.decode((self.root/('actor-'+name+'-exit.json')).read_bytes())
            self.assertEqual(note,{'actual_exit_code':0,'owned_processes_closed':True})
        self.assertFalse((self.root/'completed.json').exists())

    def test_runtime_nonzero_worker_exit_cannot_claim_completion(self):
        # Retain the existing real SIGCHLD/exit7 regression and exercise the new
        # action name with a success-shaped receipt and actual native exit7.
        source=self.root/'runtime-exit7.py'
        source.write_text('import os,sys,pathlib;sys.path.insert(0,'+repr(str(BASE))+');import diagnostic_daemon as dd\n'
            'a=sys.argv;root=pathlib.Path(__file__).resolve().parent;action="containerd"\n'
            'e=dd.Evidence(root);e.save("worker-containerd-ready.json",{"witness":dd.witness(os.getpid()),"action":action,"payload_inhibited":True})\n'
            'assert os.read(int(a[a.index("--gate-fd")+1]),128).endswith(b":containerd\\n")\n'
            'e.save("worker-containerd-complete.json",{"action":action,"actual_command_exits_zero":True,"owned_processes_closed":True});e.close();sys.exit(7)\n')
        source.chmod(0o600)
        self.p.args=types.SimpleNamespace(inventory=self.root/'input',inventory_sha256='1'*64,
            authority=self.root/'authority',authority_sha256='3'*64,source_sha256=d.sha(source.read_bytes()))
        c=d.Controller(self.p);c.evidence=self.e;c._acquire_sigchld()
        c.lock=os.open(self.root/'operation.lock',os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
        __import__('fcntl').flock(c.lock,__import__('fcntl').LOCK_EX)
        try:
            with patch.object(d,'SELF',source):c.spawn('containerd')
            with self.assertRaisesRegex(d.Refused,'worker_actual_exit'):c.wait('containerd')
            self.assertEqual(c.workers['containerd']['process'].returncode,7)
        finally:
            c.contain();c._restore_sigchld()
            for row in c.workers.values():os.close(row['pidfd'])
            if c.runtime_permission is not None:os.close(c.runtime_permission)
            os.close(c.lock)
        self.assertFalse((self.root/'completed.json').exists())

    def test_nonempty_or_rebound_client_and_rebound_socket_refuse_before_dispatch(self):
        self.private_socket()
        self.p.value['binaries']['docker'] = self.p.value['binaries']['python']
        client = self.root / 'client'; (client / 'config.json').write_text('{}')
        with self.assertRaises(d.Refused): d.execute_action(self.p, self.e, 'private-info', time.monotonic() + 3)
        self.assertEqual(self.calls, [])
        (client / 'config.json').unlink(); client.rename(self.root / 'old-client'); client.mkdir(mode=0o700)
        with self.assertRaises(d.Refused): d.execute_action(self.p, self.e, 'private-info', time.monotonic() + 3)
        self.assertEqual(self.calls, [])

    def test_socket_inode_replacement_refuses(self):
        self.private_socket(); (self.root / 'docker.sock').rename(self.root / 'old.sock')
        replacement = socket.socket(socket.AF_UNIX); self.addCleanup(replacement.close)
        replacement.bind(str(self.root / 'docker.sock'))
        with self.assertRaisesRegex(d.Refused, 'socket_rebound'): d.check_private_daemon(self.e)

    def test_default_inspect_proves_both_official_reference_and_imageid(self):
        self.p.value['binaries']['docker'] = self.p.value['binaries']['python']
        d.execute_action(self.p, self.e, 'default-inspect', time.monotonic() + 3)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0][0][-1], self.value['official_reference'])
        self.assertEqual(self.calls[1][0][-1],self.value['official_image_id'])
        for argv, kwargs in self.calls:
            self.assertEqual(kwargs['env'], d.payload_env(self.value)); self.assertEqual(kwargs['deadline'] > time.monotonic(), True)
            self.assertNotIn('HOME', kwargs['env'])

    def test_image_domain_or_arch_mismatch_never_loads_or_claims_equivalence(self):
        for changes in ({'Id': 'sha256:' + 'f' * 64}, {'Architecture': 'arm64'}, {'Size': 2 * 1024**2}):
            raw = d.canonical(dict(Id=self.value['official_image_id'],Os='linux',
                                  Architecture='amd64',Size=512,RootFS=self.fixture['diff_ids']))
            bad = d.decode(raw); bad.update(changes)
            with self.subTest(changes=changes), self.assertRaises(d.Refused): d.inspect_image(d.canonical(bad), self.p)

    def test_archive_size_type_symlink_and_late_replacement_refuse_without_deletion(self):
        path = self.write_archive()
        first = d.archive(self.p,self.e,save=True,deadline=time.monotonic()+3); self.assertEqual(first['bytes'], path.stat().st_size)
        d.archive(self.p,self.e,deadline=time.monotonic()+3)
        path.rename(self.root / 'old.tar'); path.write_bytes(b'changed'); path.chmod(0o600)
        with self.assertRaises(d.Refused): d.archive(self.p,self.e,deadline=time.monotonic()+3)
        path.unlink(); path.symlink_to(self.root / 'old.tar')
        with self.assertRaises((OSError, d.Refused)): d.archive(self.p,self.e,deadline=time.monotonic()+3)
        self.assertTrue((self.root / 'old.tar').exists())

    def test_typed_oci_identity_and_context_specific_inspection_never_use_id_fallback(self):
        path=self.write_archive();proof=d.validate_archive_image(self.p,path,deadline=time.monotonic()+3)
        for key in ('official_image_id','platform_manifest_digest','image_config_digest'):
            self.assertEqual(proof[key],self.fixture[key])
        self.assertEqual(proof['rootfs_diff_ids'],self.fixture['diff_ids'])
        self.assertNotIn(b'NEVER_OUTPUT_FIXTURE_SECRET',d.canonical(proof));self.assertNotIn(b'Env',d.canonical(proof))
        base={'Id':self.value['official_image_id'],'Os':'linux','Architecture':'amd64','Size':512,'RootFS':self.fixture['diff_ids']}
        d.inspect_image(d.canonical(base),self.p)
        private=dict(base,Id=self.value['image_config_digest'])
        d.inspect_image(d.canonical(private),self.p,private=True,expected_rootfs=self.fixture['diff_ids'])
        with self.assertRaises(d.Refused):d.inspect_image(d.canonical(private),self.p)
        with self.assertRaises(d.Refused):d.inspect_image(d.canonical(base),self.p,private=True,expected_rootfs=self.fixture['diff_ids'])
        with self.assertRaisesRegex(d.Refused,'private_image_rootfs_changed'):
            d.inspect_image(d.canonical(dict(private,RootFS=['sha256:'+'0'*64])),self.p,private=True,expected_rootfs=self.fixture['diff_ids'])
        default=d.action_commands(self.p,'default-inspect')
        self.assertEqual([argv[-1] for _,argv in default],[self.value['official_reference'],self.value['official_image_id']])
        self.assertEqual(d.action_commands(self.p,'save-image')[0][1][-1],self.value['official_image_id'])
        self.assertEqual(d.action_commands(self.p,'private-inspect')[0][1][-1],self.value['image_config_digest'])

    def test_oci_wrong_native_branch_config_or_media_refuses_even_with_bound_hashes(self):
        cases=[oci_fixture(duplicate_native=True),oci_fixture(native_platform={'os':'unknown','architecture':'unknown'}),
               oci_fixture(config_os='windows'),oci_fixture(config_arch='arm64'),
               oci_fixture(layer_media='application/vnd.in-toto+json'),oci_fixture(config_padding='x'*70000),
               oci_fixture(legacy_tags=['arbitrary:tag'])]
        for n,fixture in enumerate(cases):
            with self.subTest(case=n):
                path=self.write_archive(fixture)
                with self.assertRaises(d.Refused):d.validate_archive_image(self.p,path,deadline=time.monotonic()+3)

    def test_oci_hash_layer_presence_and_legacy_mapping_are_all_required(self):
        for change in ('config_bytes','layer_bytes','missing_layer','legacy_config','legacy_layer','multiple_roots'):
            files=copy.deepcopy(self.fixture['files'])
            if change=='config_bytes':files[self.fixture['config_path']]+=b' '
            elif change=='layer_bytes':files[self.fixture['layer_path']]+=b' '
            elif change=='missing_layer':files.pop(self.fixture['layer_path'])
            elif change=='multiple_roots':
                value=d.decode(files['index.json']);value['manifests'].append(copy.deepcopy(value['manifests'][0]));files['index.json']=d.canonical(value)
            else:
                value=d.decode(files['manifest.json'])
                if change=='legacy_config':value[0]['Config']='blobs/sha256/'+'0'*64
                else:value[0]['Layers']=[self.fixture['config_path']]
                files['manifest.json']=d.canonical(value)
            with self.subTest(change=change):
                path=self.write_archive(files=files)
                with self.assertRaises(d.Refused):d.validate_archive_image(self.p,path,deadline=time.monotonic()+3)

    def test_oci_duplicate_traversal_link_and_foreign_members_never_extract(self):
        for change in ('duplicate','traversal','symlink','foreign'):
            if change=='duplicate':item=tarfile.TarInfo(self.fixture['config_path']);raw=self.fixture['files'][item.name]
            elif change=='traversal':item=tarfile.TarInfo('../not-created');raw=b'x'
            elif change=='symlink':item=tarfile.TarInfo('blobs/sha256/'+'0'*64);item.type=tarfile.SYMTYPE;item.linkname='/tmp/not-followed';raw=None
            else:item=tarfile.TarInfo('untrusted-extra');raw=b'x'
            if raw is not None:item.size=len(raw)
            with self.subTest(change=change):
                path=self.write_archive(extras=[(item,raw)])
                with self.assertRaises(d.Refused):d.validate_archive_image(self.p,path,deadline=time.monotonic()+3)
        self.assertFalse((self.root/'not-created').exists())

    def test_oci_member_bound_is_fail_closed(self):
        self.value['image_archive_max_bytes']=8*1024**2
        extras=[]
        for n in range(d.MAX_ARCHIVE_MEMBERS):
            item=tarfile.TarInfo('blobs/sha256/'+format(n,'064x'));item.size=0;extras.append((item,b''))
        path=self.write_archive(extras=extras)
        with patch.object(d.tarfile,'open',side_effect=AssertionError('physical bound precedes stdlib')) as parser:
            with self.assertRaisesRegex(d.Refused,'archive_physical_bound'):
                d.validate_archive_image(self.p,path,deadline=time.monotonic()+5)
            parser.assert_not_called()

    def test_hidden_pax_and_gnu_extensions_refuse_before_stdlib_allocation(self):
        path=self.write_archive()
        cases=[tar_fixture(self.fixture['files'],pax_headers={'comment':'x'*70000})]
        for kind in (b'x',b'g',b'L',b'K',b'S',b'Z'):
            item=tarfile.TarInfo('hidden-extension');item.type=kind;item.size=1024**3
            cases.append(item.tobuf(format=tarfile.USTAR_FORMAT)+b'\0'*1024)
        for n,raw in enumerate(cases):
            path.write_bytes(raw);path.chmod(0o600)
            with self.subTest(case=n),patch.object(d.tarfile,'open',side_effect=AssertionError('extension must never reach stdlib')) as parser:
                with self.assertRaisesRegex(d.Refused,'archive_physical_type'):
                    d.validate_archive_image(self.p,path,deadline=time.monotonic()+3)
                parser.assert_not_called()

    def test_physical_header_numeric_offset_padding_and_trailer_bounds(self):
        path=self.write_archive();valid=path.read_bytes();cases=[]
        def changed(field,value):
            item=tarfile.TarInfo('index.json');item.size=0;header=bytearray(item.tobuf(format=tarfile.USTAR_FORMAT))
            header[field]=value;header[148:156]=b' '*8
            header[148:156]=('%06o\0 '%sum(header)).encode();return bytes(header)+b'\0'*1024
        cases.append(changed(slice(124,136),b'\x80'+b'\0'*10+b'\1'))
        cases.append(changed(slice(124,136),b'99999999999\0'))
        cases.append(changed(slice(124,136),b'00000010000\0'))
        cases.append(changed(slice(124,136),('%011o\0'%(2*1024**2)).encode()))
        cases.append(valid[:512*3])
        cases.append(valid[:-1])
        with tarfile.open(fileobj=io.BytesIO(valid),mode='r:') as archive:
            row=archive.getmember('oci-layout');end=row.offset_data+row.size
        padding=bytearray(valid);padding[end]=1;cases.append(bytes(padding))
        trailer=bytearray(valid);trailer[-1]=1;cases.append(bytes(trailer))
        for n,raw in enumerate(cases):
            path.write_bytes(raw);path.chmod(0o600)
            with self.subTest(case=n),patch.object(d.tarfile,'open',side_effect=AssertionError('bad physical framing before stdlib')) as parser:
                with self.assertRaises(d.Refused):d.validate_archive_image(self.p,path,deadline=time.monotonic()+3)
                parser.assert_not_called()

    def test_physical_header_prepass_keeps_original_deadline_after_read(self):
        raw=tar_fixture(self.fixture['files']);clock=types.SimpleNamespace(now=1.0)
        class LateRead(io.BytesIO):
            def read(inner,count):
                value=super().read(count);clock.now=3.0;return value
        with patch.object(d.time,'monotonic',side_effect=lambda:clock.now):
            with self.assertRaisesRegex(d.Refused,'image_identity_deadline'):
                d._tar_physical_members(LateRead(raw),len(raw),self.value['image_archive_max_bytes'],2.0)

    def test_archive_original_deadline_and_late_file_mutation_refuse_proof(self):
        path=self.write_archive();original=d._archive_identity
        clock=types.SimpleNamespace(now=1.0)
        def late(*args):
            value=original(*args);clock.now=3.0;return value
        with patch.object(d.time,'monotonic',side_effect=lambda:clock.now),patch.object(d,'_archive_identity',side_effect=late):
            with self.assertRaisesRegex(d.Refused,'image_identity_deadline'):
                d.validate_archive_image(self.p,path,deadline=2.0)
        def changed(*args):
            value=original(*args);data=path.read_bytes();path.write_bytes(data);return value
        with patch.object(d,'_archive_identity',side_effect=changed):
            with self.assertRaisesRegex(d.Refused,'archive_changed'):
                d.validate_archive_image(self.p,path,deadline=time.monotonic()+3)
        self.assertFalse((self.root/'image-archive.json').exists())

    def test_archive_rootfs_must_match_actual_default_inspection(self):
        path=self.write_archive();note=self.root/'default-image-binding.json';value=d.decode(note.read_bytes())
        value['inspections'][0]['RootFS']=['sha256:'+'0'*64];note.write_bytes(d.canonical(value));note.chmod(0o600)
        with self.assertRaisesRegex(d.Refused,'archive_default_rootfs_changed'):
            d.archive(self.p,self.e,deadline=time.monotonic()+3,save=True)
        self.assertFalse((self.root/'image-archive.json').exists())

    def test_archive_provenance_written_only_after_actual_save_exit_zero(self):
        self.p.value['binaries']['docker'] = self.p.value['binaries']['python']
        def save(argv, **kwargs):
            self.calls.append(argv); path = self.root / 'image.tar'; path.write_bytes(b'partial'); path.chmod(0o600)
            return result(actual_exit_code=1)
        self.p.process.execute = save
        with self.assertRaises(d.Refused): d.execute_action(self.p, self.e, 'save-image', time.monotonic() + 3)
        self.assertTrue((self.root / 'image.tar').exists()); self.assertFalse((self.root / 'image-archive.json').exists())
        self.assertEqual(len(self.calls), 1)

    def test_hashed_executable_fd_and_predispatch_intent(self):
        self.p.value['binaries']['docker'] = self.p.value['binaries']['python']
        def actual(argv, **kwargs):
            self.assertTrue((self.root / 'worker-default-inspect-0-intent.json').exists())
            self.assertEqual(os.fstat(kwargs['executable_fd']).st_ino, Path(sys.executable).resolve().stat().st_ino)
            return self.execute(argv, **kwargs)
        self.p.process.execute = actual
        d.execute_action(self.p, self.e, 'default-inspect', time.monotonic() + 3)

    def test_private_empty_includes_untagged_image_hidden_from_default_list(self):
        self.private_socket();self.p.value['binaries']['docker']=self.p.value['binaries']['python']
        expected=self.p.hv['native_pg_image']['image_id']
        def native_list(argv,**kwargs):
            self.calls.append(argv)
            if 'ps' in argv:return result(stdout=b'')
            self.assertIn('image',argv);self.assertIn('ls',argv)
            # Reproduce native Docker's untagged-image visibility, so omitting
            # --all really fails the exact-resource proof instead of this
            # fixture merely asserting that a literal flag is present.
            return result(stdout=(expected+'\n').encode() if '--all' in argv else b'')
        self.p.process.execute=native_list
        d.execute_action(self.p,self.e,'private-empty',time.monotonic()+3)
        self.assertEqual(len(self.calls),2)
        self.assertTrue((self.root/'worker-private-empty-complete.json').exists())

    def test_private_empty_still_rejects_extra_untagged_image(self):
        self.private_socket();self.p.value['binaries']['docker']=self.p.value['binaries']['python']
        expected=self.p.hv['native_pg_image']['image_id']
        def native_list(argv,**kwargs):
            if 'ps' in argv:return result(stdout=b'')
            self.assertIn('--all',argv)
            return result(stdout=(expected+'\nsha256:'+'f'*64+'\n').encode())
        self.p.process.execute=native_list
        with self.assertRaisesRegex(d.Refused,'private_resources_not_exact'):
            d.execute_action(self.p,self.e,'private-empty',time.monotonic()+3)
        self.assertFalse((self.root/'worker-private-empty-complete.json').exists())

    def test_private_empty_still_rejects_remaining_container(self):
        self.private_socket();self.p.value['binaries']['docker']=self.p.value['binaries']['python']
        expected=self.p.hv['native_pg_image']['image_id']
        def native_list(argv,**kwargs):
            if 'ps' in argv:return result(stdout=('c'*64+'\n').encode())
            self.assertIn('--all',argv);return result(stdout=(expected+'\n').encode())
        self.p.process.execute=native_list
        with self.assertRaisesRegex(d.Refused,'private_resources_not_exact'):
            d.execute_action(self.p,self.e,'private-empty',time.monotonic()+3)
        self.assertFalse((self.root/'worker-private-empty-complete.json').exists())

    def test_go_gate_owner_record_precedes_real_harmless_worker_effect(self):
        source = self.root / 'worker.py'
        source.write_text('import sys;sys.path.insert(0,' + repr(str(BASE)) + ');import diagnostic_daemon as dd\n' + '''import argparse,json,os,pathlib
p=argparse.ArgumentParser();p.add_argument('command')
for k in ('worker-action','gate-fd','deadline','inventory','inventory-sha256','authority','authority-sha256','source-sha256'):p.add_argument('--'+k)
a=p.parse_args();root=pathlib.Path(__file__).resolve().parent;action=a.worker_action
e=dd.Evidence(root);e.save('worker-'+action+'-ready.json',{'witness':dd.witness(os.getpid()),'action':action,'payload_inhibited':True});e.close()
assert os.read(int(a.gate_fd),128).endswith((':'+action+'\\n').encode())
assert (root/('worker-'+action+'-owner.json')).exists()
(root/'harmless-effect').write_text('gate observed')
raw=json.dumps({'action':action,'actual_command_exits_zero':True,'owned_processes_closed':True})+'\\n'
out=root/('worker-'+action+'-complete.json');out.write_text(raw);out.chmod(0o600)
'''); source.chmod(0o600)
        self.p.args = types.SimpleNamespace(inventory=self.root / 'input', inventory_sha256='1' * 64,
            authority=self.root / 'authority', authority_sha256='3' * 64, source_sha256=d.sha(source.read_bytes()))
        controller = d.Controller(self.p); controller.evidence = self.e
        controller._acquire_sigchld()
        with patch.object(d, 'SELF', source):
            controller.action('default-inspect')
        controller._restore_sigchld()
        row = controller.workers['default-inspect']; self.addCleanup(os.close, row['pidfd'])
        self.assertEqual(row['process'].returncode, 0); self.assertEqual((self.root / 'harmless-effect').read_text(), 'gate observed')
        self.assertFalse(d.alive(row['pidfd'])); self.assertEqual(command_process._children(os.getpid()), set())

    def test_sigchld_ignored_actual_exit7_is_refused_despite_success_shaped_receipt(self):
        source=self.root/'exit7-worker.py'
        source.write_text('import os,sys,pathlib;sys.path.insert(0,'+repr(str(BASE))+');import diagnostic_daemon as dd\n'
            'a=sys.argv;root=pathlib.Path(__file__).resolve().parent;action=a[a.index("--worker-action")+1]\n'
            'e=dd.Evidence(root);e.save("worker-"+action+"-ready.json",{"witness":dd.witness(os.getpid()),"action":action,"payload_inhibited":True})\n'
            'assert os.read(int(a[a.index("--gate-fd")+1]),128).endswith((":"+action+"\\n").encode())\n'
            'e.save("worker-"+action+"-complete.json",{"action":action,"actual_command_exits_zero":True,"owned_processes_closed":True});e.close();os._exit(7)\n')
        source.chmod(0o600)
        args={'inventory':str(self.root/'input'),'inventory_sha256':'1'*64,
              'authority':str(self.root/'authority'),'authority_sha256':'3'*64,'source_sha256':d.sha(source.read_bytes())}
        code=f"""import sys,types,signal,json,os
from pathlib import Path
sys.path.insert(0,{str(BASE)!r});import diagnostic_daemon as d;import command_process
d.SELF=Path({str(source)!r});d.seal=lambda *a:None
p=types.SimpleNamespace(value={self.value!r},args=types.SimpleNamespace(**{args!r}),process=command_process)
signal.signal(signal.SIGCHLD,signal.SIG_IGN)
c=d.Controller(p);c.evidence=d.Evidence({str(self.root)!r});c._acquire_sigchld()
row=c.spawn('default-inspect')
try:c._restore_sigchld()
except d.Refused:early_restore_refused=True
else:early_restore_refused=False
assert signal.getsignal(signal.SIGCHLD)==signal.SIG_DFL
try:c.wait('default-inspect')
except d.Refused as error:accepted=False;reason=str(error)
else:accepted=True;reason='accepted'
c.contain();c._restore_sigchld()
print(json.dumps({{'actual_exit':row['process'].returncode,'accepted':accepted,'reason':reason,
 'early_restore_refused':early_restore_refused,'previous_sigign_restored':signal.getsignal(signal.SIGCHLD)==signal.SIG_IGN}}))
os.close(row['pidfd']);c.evidence.close()
"""
        run=subprocess.run([sys.executable,'-I','-B','-c',code],capture_output=True,timeout=10)
        self.assertEqual(run.returncode,0,run.stderr.decode())
        observed=d.decode(run.stdout)
        self.assertEqual(observed,{'actual_exit':7,'accepted':False,'reason':'worker_actual_exit',
            'early_restore_refused':True,'previous_sigign_restored':True})
        receipt=d.decode((self.root/'worker-default-inspect-os-exit.json').read_bytes())
        self.assertEqual(receipt['actual_exit_code'],7)
        self.assertTrue((self.root/'worker-default-inspect-complete.json').exists())
        self.assertFalse((self.root/'completed.json').exists())

    def test_intent_failure_keeps_go_gate_closed(self):
        source = self.root / 'worker.py'
        source.write_text('import os,sys,pathlib;sys.path.insert(0,' + repr(str(BASE)) + ');import diagnostic_daemon as dd\n'
            'a=sys.argv;root=pathlib.Path(__file__).resolve().parent;action=a[a.index("--worker-action")+1]\n'
            'e=dd.Evidence(root);e.save("worker-"+action+"-ready.json",{"witness":dd.witness(os.getpid()),"action":action,"payload_inhibited":True});e.close()\n'
            'f=int(a[a.index("--gate-fd")+1]); assert os.read(f,128)==b""\n')
        source.chmod(0o600)
        self.p.args = types.SimpleNamespace(inventory=self.root / 'input', inventory_sha256='1' * 64,
            authority=self.root / 'authority', authority_sha256='3' * 64, source_sha256=d.sha(source.read_bytes()))
        c = d.Controller(self.p); c.evidence = self.e
        c._acquire_sigchld()
        with patch.object(d, 'SELF', source), patch.object(self.e, 'save', side_effect=OSError('synthetic fsync failure')):
            with self.assertRaises(OSError): c.spawn('default-inspect')
        row = c.workers['default-inspect']; self.addCleanup(os.close, row['pidfd'])
        self.assertEqual(row['process'].wait(timeout=self.value['total_deadline_secs']), 0)
        c._restore_sigchld()
        self.assertFalse((self.root / 'harmless-effect').exists())

    def script(self, child_code, action='daemon'):
        # A dedicated subprocess runs the real owned-command helper against ONLY
        # the trusted local Python executable; action grammar is patched in this
        # fixture, never exposed as an inventory command hook.
        py = self.value['binaries']['python']
        return f'''import sys,types,time,importlib.util
from pathlib import Path
sys.path.insert(0,{str(BASE)!r});import command_process
s=importlib.util.spec_from_file_location('d',{str(BASE / 'diagnostic_daemon.py')!r});d=importlib.util.module_from_spec(s);s.loader.exec_module(d)
d.seal=lambda *a:None
d.check_runtime_binaries=lambda *a:None;d.check_private_runtime=lambda *a:None
p=types.SimpleNamespace(value={self.value!r},args=types.SimpleNamespace(inventory_sha256='1'*64,source_sha256='2'*64),process=command_process)
d.action_commands=lambda *a:[('python',['-I','-B','-c',{child_code!r}])]
e=d.Evidence({str(self.root)!r})
try:d.execute_action(p,e,{action!r},time.monotonic()+10)
finally:e.close()
'''

    def test_real_owned_process_normal_shutdown_is_observed_and_reaped(self):
        path = self.root / 'harmless.pid'
        child = 'import os,signal,pathlib;signal.signal(signal.SIGTERM,lambda *a:exit(0));pathlib.Path(' + repr(str(path)) + ').write_text(str(os.getpid()));signal.pause()'
        process = subprocess.Popen([sys.executable, '-I', '-B', '-c', self.script(child)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(lambda: process.poll() is None and process.kill())
        end = time.monotonic() + 4
        while not path.exists() and process.poll() is None and time.monotonic() < end: time.sleep(.02)
        self.assertTrue(path.exists()); pid = int(path.read_text()); fd = os.pidfd_open(pid)
        try: signal.pidfd_send_signal(fd, signal.SIGTERM, None, 0)
        finally: os.close(fd)
        stdout, stderr = process.communicate(timeout=12)
        self.assertEqual(process.returncode, 0, (stdout, stderr)); self.assertFalse(Path('/proc/' + str(pid)).exists())
        record = d.decode((self.root / 'worker-daemon-0-exit.json').read_bytes())
        self.assertEqual(record['actual_exit_code'], 0); self.assertTrue(record['owned_processes_closed'])
        self.assertFalse(record['unexpected_descendants']); self.assertTrue((self.root / 'worker-daemon-complete.json').exists())

    def test_real_setsid_descendant_refuses_success_after_actual_cleanup(self):
        path = self.root / 'escaped.pid'
        child = ('import os,time,pathlib;p=os.fork()\nif p==0:\n os.setsid();os.close(0);os.close(1);os.close(2);'
                 'pathlib.Path(' + repr(str(path)) + ').write_text(str(os.getpid()));time.sleep(30);os._exit(0)\n'
                 'else:time.sleep(.1);os._exit(0)')
        run = subprocess.run([sys.executable, '-I', '-B', '-c', self.script(child)], capture_output=True, timeout=15)
        self.assertNotEqual(run.returncode, 0); self.assertTrue(path.exists())
        self.assertFalse(Path('/proc/' + path.read_text()).exists())
        record = d.decode((self.root / 'worker-daemon-0-exit.json').read_bytes())
        self.assertTrue(record['owned_processes_closed']); self.assertTrue(record['unexpected_descendants'])
        self.assertFalse((self.root / 'worker-daemon-complete.json').exists())

    def test_controller_synthetic_order_requires_benchmark_and_shutdown_before_pending_outer_exit(self):
        c = d.Controller(self.p); events = []
        c.setup = lambda: setattr(c, 'evidence', self.e)
        c.action = lambda name: events.append(name)
        c.spawn = lambda name: events.append('spawn-' + name)
        fd = os.open('/dev/null', os.O_RDONLY)
        runtimefd = os.open('/dev/null', os.O_RDONLY)
        c.bind_runtime = lambda: setattr(c, 'runtime', {'pidfd': runtimefd, 'witness': d.witness(os.getpid())})
        c.bind_daemon = lambda: setattr(c, 'daemon', {'pidfd': fd, 'witness': d.witness(os.getpid())})
        c.wait = lambda name: events.append('wait-' + name)
        c.permit_runtime_stop = lambda:events.append('permit-runtime')
        with patch.object(d, 'signal_owned', side_effect=lambda *a: events.append('signal-private')), \
             patch.object(d, 'alive', return_value=False):
            value = c.run()
        self.assertEqual(events, ['default-inspect', 'save-image', 'runtime-profile', 'spawn-containerd',
            'spawn-daemon', 'private-info', 'load-image', 'private-inspect', 'benchmark', 'private-empty',
            'signal-private', 'wait-daemon', 'permit-runtime', 'wait-containerd'])
        self.assertEqual(value['status'], 'COMPLETE_PENDING_OUTER_OS_EXIT')
        self.assertEqual(value['outer_actual_os_exit'], 'NOT_YET_OBSERVABLE')

    def test_controller_failed_benchmark_never_runs_empty_or_claims_completion_and_retains_files(self):
        c = d.Controller(self.p); events = []
        c.setup = lambda: setattr(c, 'evidence', self.e)
        def action(name):
            events.append(name)
            if name == 'benchmark': raise RuntimeError('synthetic unknown benchmark exit')
        c.action = action; c.spawn = lambda name: events.append('spawn-' + name)
        fd = os.open('/dev/null', os.O_RDONLY)
        runtimefd = os.open('/dev/null', os.O_RDONLY)
        c.bind_runtime = lambda: setattr(c, 'runtime', {'pidfd': runtimefd, 'witness': d.witness(os.getpid())})
        c.bind_daemon = lambda: setattr(c, 'daemon', {'pidfd': fd, 'witness': d.witness(os.getpid())})
        with patch.object(d, 'signal_owned', side_effect=lambda *a: events.append('contain-private')), \
             patch.object(d, 'alive', return_value=False):
            with self.assertRaisesRegex(RuntimeError, 'unknown benchmark'): c.run()
        self.assertNotIn('private-empty', events); self.assertEqual(events.count('contain-private'), 1)
        self.assertFalse((self.root / 'completed.json').exists())
        failed = d.decode((self.root / 'failed.json').read_bytes())
        self.assertEqual(failed['status'], 'FAILED_OR_UNKNOWN'); self.assertTrue(failed['files_and_datasets_retained'])
        self.assertTrue((self.root / 'daemon.json').exists())

    def test_crashed_daemon_worker_does_not_authorize_runtime_shutdown_while_native_daemon_lives(self):
        c=d.Controller(self.p);c.evidence=self.e;c.deadline=time.monotonic()-1
        c._child_context=lambda:None
        c.daemon={'pidfd':101,'witness':{'pid':201}}
        c.runtime={'pidfd':102,'witness':{'pid':202}}
        c.workers['daemon']={'process':types.SimpleNamespace(returncode=-9,poll=lambda:-9,wait=lambda:-9)}
        events=[]
        with patch.object(d,'alive',side_effect=lambda fd:True),\
             patch.object(d,'signal_owned',side_effect=lambda row,_:events.append(row['pidfd'])):
            with self.assertRaisesRegex(d.Refused,'daemon_closure_unknown_runtime_retained'):c.contain()
        self.assertEqual(events,[101])
        raw=d.decode((self.root/'containment-runtime-retained.json').read_bytes())
        self.assertFalse(raw['runtime_shutdown_dispatched']);self.assertTrue(raw['ownership_retained'])
        self.assertEqual(raw['daemon_physical_closure'],'UNKNOWN')
        self.assertFalse((self.root/'completed.json').exists())

    def test_containment_waits_native_daemon_pidfd_before_runtime_signal(self):
        c=d.Controller(self.p);c.evidence=self.e;c._child_context=lambda:None
        c.daemon={'pidfd':101,'witness':{'pid':201}};c.runtime={'pidfd':102,'witness':{'pid':202}}
        c.workers['daemon']={'process':types.SimpleNamespace(returncode=-9,poll=lambda:-9,wait=lambda:-9)}
        state={'daemon_alive':True};events=[]
        def pause(_):
            events.append('native-exit-observed-next');state['daemon_alive']=False
        def signal(row,_):
            events.append((row['pidfd'],state['daemon_alive']))
        with patch.object(d,'alive',side_effect=lambda fd:state['daemon_alive'] if fd==101 else False),\
             patch.object(d,'signal_owned',side_effect=signal),patch.object(d.time,'sleep',side_effect=pause):
            result=c.contain()
        self.assertEqual(events,[(101,True),'native-exit-observed-next'])
        self.assertTrue(result['physical_daemon_closure'])

    def test_unbound_daemon_after_worker_crash_retains_runtime_without_guessing_a_target(self):
        c=d.Controller(self.p);c.evidence=self.e;c._child_context=lambda:None
        c.runtime={'pidfd':102,'witness':{'pid':202}}
        c.workers['daemon']={'process':types.SimpleNamespace(returncode=-9,poll=lambda:-9,wait=lambda:-9)}
        with patch.object(d,'signal_owned',side_effect=AssertionError('no guessed signal')):
            with self.assertRaisesRegex(d.Refused,'daemon_identity_unknown_runtime_retained'):c.contain()
        self.assertFalse((self.root/'completed.json').exists())

    def test_runtime_stop_requires_worker_owned_descendant_proof_not_leader_exit_alone(self):
        c=d.Controller(self.p);c.evidence=self.e;c._child_context=lambda:None
        c.daemon={'pidfd':101,'witness':{'pid':201}}
        c.workers['daemon']={'process':types.SimpleNamespace(returncode=-9),'pidfd':103}
        readfd,writefd=os.pipe2(os.O_CLOEXEC);c.runtime_permission=writefd
        try:
            with patch.object(d,'alive',return_value=False):
                with self.assertRaises(FileNotFoundError):c.permit_runtime_stop()
            os.set_blocking(readfd,False)
            with self.assertRaises(BlockingIOError):os.read(readfd,128)
            self.e.save('worker-daemon-0-exit.json',{'started':True,'actual_exit_code':0,'owned_processes_closed':False})
            with patch.object(d,'alive',return_value=False),self.assertRaisesRegex(d.Refused,'descendant_closure_unknown'):
                c.permit_runtime_stop()
            self.assertFalse(c.runtime_stop_sent)
            self.assertFalse((self.root/'containerd-stop-permission-intent.json').exists())
        finally:os.close(readfd);os.close(writefd)

    def test_unowned_pidfile_never_becomes_a_containment_target(self):
        c = d.Controller(self.p); c.evidence = self.e
        c._acquire_sigchld()
        self.addCleanup(c._restore_sigchld)
        (self.root / 'docker.pid').write_text(str(os.getpid())); (self.root / 'docker.pid').chmod(0o600)
        c.workers['daemon'] = {'process': types.SimpleNamespace(pid=os.getpid() + 100000, poll=lambda: None,returncode=0),'pidfd':None}
        with self.assertRaisesRegex(d.Refused, 'process_parent'): c.bind_daemon()
        self.assertIsNone(c.daemon)

    def test_harmless_archive_writer_cannot_exceed_inherited_rlimit(self):
        path = self.root / 'image.tar'
        child = 'import pathlib;pathlib.Path(' + repr(str(path)) + ').write_bytes(b"x"*2097152)'
        code = self.script(child, 'save-image')
        # Executable child inherits the cap; an overflowing save never publishes
        # an archive provenance receipt, even if a partial tar file exists.
        code = 'import resource;resource.setrlimit(resource.RLIMIT_FSIZE,(262144,262144))\n' + code
        run = subprocess.run([sys.executable, '-I', '-B', '-c', code], capture_output=True, timeout=12)
        self.assertNotEqual(run.returncode, 0); self.assertTrue(path.exists())
        self.assertLessEqual(path.stat().st_size, 262144)
        self.assertFalse((self.root / 'image-archive.json').exists())


if __name__ == '__main__': unittest.main(verbosity=2)
