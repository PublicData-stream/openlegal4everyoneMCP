"""Standalone diagnostic tests: disposable /tmp files and harmless local children.
No ZFS/Docker commands, image execution, provider calls or network operations.
"""
import contextlib
import copy
import importlib.util
import io
import os
import stat
import socket
import sys
import types
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / 'hdd_benchmark.py'
spec = importlib.util.spec_from_file_location('hdd_benchmark', SOURCE)
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)


def inventory(operation):
    return {'schema_version': 1, 'kind': 'openlegal_hdd_benchmark_inventory', 'revision': '1' * 40,
            'pool': {'name': 'syntheticpool', 'guid': '1234567', 'reserve_bytes':4096, 'leaf_devices': [
                {'path': '/dev/synthetic0', 'major': 8, 'minor': 16,
                 'sysfs_path': '/sys/devices/synthetic/block/synthetic0',
                 'identity_file': 'wwid', 'identity_sha256': 'a' * 64}]},
            'binaries': {n: {'path': '/usr/sbin/' + n, 'sha256': 'b' * 64}
                         for n in ('zfs', 'zpool', 'docker')},
            'native_pg_image': {'reference': 'example.invalid/synthetic@sha256:' + 'c' * 64,
                                'image_id': 'sha256:' + 'd' * 64, 'os': 'linux',
                                'architecture': 'amd64', 'major': 18, 'uid': 70, 'gid': 70},
            'evidence_root': '/tmp/openlegal-hdd-benchmark-' + operation,
            'total_deadline_secs': 1200, 'host_boot_id': str(uuid.uuid4()),
            'process_sources':{'interface.py':'a'*64,'command_process.py':'b'*64},
            'docker':{'endpoint':'unix:///var/run/docker.sock','root_dir':'/tmp/docker/rootful'}}


def rounds():
    result = []
    for number in range(3):
        for size, latency in ((128, 10000), (32, 7000)):
            blob = {str(b): {'budget_secs': b, 'expiries': 0, 'physical_calls_completed': True,
                            'phases_us': {p: [latency] * 128 for p in h.PHASES}} for b in (5, 30)}
            result.append({'round': number, 'recordsize': size, 'commits_us': [latency] * 128,
                           'client_roundtrip_us': latency * 128, 'blob': blob})
    return result


class SchemaTests(unittest.TestCase):
    def setUp(self):
        self.operation = str(uuid.uuid4())
        self.value = inventory(self.operation)

    def test_import_is_inert(self):
        with patch('os.mkdir', side_effect=AssertionError('no writes on import')), \
             patch('subprocess.Popen', side_effect=AssertionError('no actors on import')):
            fresh = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(fresh)

    def test_fixed_plan_only_fresh_128_and32_same_safe_properties(self):
        plan = h.plan(self.value, self.operation)
        self.assertEqual(plan['round_order'], [128, 32, 32, 128, 128, 32])
        self.assertEqual(plan['maximum_live_payload_bytes'], 2 * 1024**3)
        for row in plan['datasets']:
            self.assertTrue(row['name'].startswith('syntheticpool/openlegal-hdd-benchmark-' + self.operation + '/'))
            self.assertTrue(row['mountpoint'].startswith(self.value['evidence_root'] + '/'))
            self.assertEqual((row['compression'], row['sync'], row['atime'], row['quota']),
                             ('lz4', 'standard', 'off', 1024**3))
        self.assertFalse(plan['production_tuning_permitted'])

    def test_arbitrary_commands_names_paths_and_budgets_refuse(self):
        changes = [dict(self.value, evidence_root='/mnt/production'),
                   dict(self.value, total_deadline_secs=1201),
                   dict(self.value, pool=dict(self.value['pool'], name='pool/production')),
                   dict(self.value, binaries={**self.value['binaries'], 'shell': {'path': '/bin/sh', 'sha256': 'a'*64}}),
                   dict(self.value, native_pg_image=dict(self.value['native_pg_image'], image_id='postgres:18')),
                   dict(self.value, native_pg_image=dict(self.value['native_pg_image'], major=17)),
                   dict(self.value, unknown_command=['destroy', '-r', 'production'])]
        for value in changes:
            with self.subTest(value=value), self.assertRaises(h.Refused):
                h.validate_inventory(value, self.operation)

    def test_decoder_duplicates_nonfinite_and_depth_refuse(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'['*30+b'0'+b']'*30):
            with self.assertRaises(h.Refused): h.decode(raw)

    def test_approval_binds_every_scope_and_never_production_tuning(self):
        value = {'schema_version': 1, 'kind': 'openlegal_hdd_benchmark_authority', 'approved': True,
                 'operation_id': self.operation, 'inventory_sha256': 'a'*64, 'source_sha256': 'b'*64,
                 'plan_sha256': h.sha(h.canonical(h.plan(self.value, self.operation))),
                 'pool_guid': self.value['pool']['guid'], 'valid_until': time.time()+600,
                 'fresh_test_datasets_only': True, 'destruction_of_exact_owned_test_datasets': True,
                 'production_tuning_permitted': False, 'provider_calls_permitted': False}
        h.validate_approval(value, self.value, 'a'*64, 'b'*64, self.operation)
        for changes in ({'approved': False}, {'source_sha256': 'c'*64}, {'pool_guid': '9'},
                        {'production_tuning_permitted': True}, {'provider_calls_permitted': True},
                        {'destruction_of_exact_owned_test_datasets': False}, {'valid_until': 1}):
            with self.subTest(changes=changes), self.assertRaises(h.Refused):
                h.validate_approval(dict(value, **changes), self.value, 'a'*64, 'b'*64, self.operation)

    def test_pool_parser_excludes_read_cache_but_includes_logs_and_metadata_devices(self):
        raw=b'pool ONLINE\n mirror-0 ONLINE\n /dev/a ONLINE\n /dev/b ONLINE\nspecial\n /dev/c ONLINE\nlogs\n /dev/log ONLINE\ncache\n /dev/l2arc ONLINE\n'
        self.assertEqual(h.pool_leaves(raw), {'/dev/a', '/dev/b', '/dev/c', '/dev/log'})


class ComparisonTests(unittest.TestCase):
    def test_three_repeatable_pairs_and_both_blob_budgets_required(self):
        result = h.assess(rounds())
        self.assertTrue(result['candidate_32k_repeatably_better'])
        self.assertFalse(result['production_tuning_permitted'])
        self.assertFalse(result['capacity_or_causality_proven'])

    def test_no_improvement_is_inconclusive_and_preserves128(self):
        values = rounds()
        values[-1]['commits_us'] = [9000]*128
        result = h.assess(values)
        self.assertFalse(result['candidate_32k_repeatably_better'])
        self.assertEqual(result['recommendation'], 'preserve_128k_inconclusive')

    def test_blob_gain_cannot_be_hidden_by_sql_gain(self):
        values = rounds()
        values[1]['blob']['30']['phases_us']['total'] = [10000]*128
        self.assertFalse(h.assess(values)['candidate_32k_repeatably_better'])

    def test_expiry_max_outlier_and_client_queue_overhead_refuse_candidate(self):
        for kind in ('expiry', 'outlier', 'client', 'phase'):
            values = rounds()
            if kind == 'expiry': values[1]['blob']['5']['expiries'] = 1
            elif kind == 'outlier': values[1]['blob']['5']['phases_us']['total'][0] = 4_000_000
            elif kind == 'client': values[1]['client_roundtrip_us'] += 2_000_000
            else: values[1]['blob']['30']['phases_us']['file_fsync'] = [16_000_000]*128
            with self.subTest(kind=kind):
                self.assertFalse(h.assess(values)['candidate_32k_repeatably_better'])

    def test_missing_reordered_or_unclosed_samples_are_not_accepted(self):
        for change in ('missing', 'duplicate', 'unclosed', 'bool'):
            values = rounds()
            if change == 'missing': values.pop()
            elif change == 'duplicate': values[-1]['round'] = 1
            elif change == 'unclosed': values[0]['blob']['5']['physical_calls_completed'] = False
            else: values[0]['commits_us'][0] = True
            with self.subTest(change=change), self.assertRaises(h.Refused): h.assess(values)

    def test_actual_commit_notice_parser_requires128_ordered_matching_samples(self):
        data = '\n'.join(f'NOTICE:  openlegal_hdd_commit:2:{i}:100' for i in range(1, 129)).encode()
        self.assertEqual(h.latency_samples(data, 2), [100]*128)
        for raw in (data.replace(b':2:2:', b':2:1:'), data.replace(b':2:', b':1:', 1), data[:-45]):
            with self.assertRaises(h.Refused): h.latency_samples(raw, 2)


class FilesystemTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='openlegal-hdd-diagnostic-tests-', dir='/tmp')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.root.chmod(0o700)

    def test_real_tiny_probe_has_complete_phases_and_no_leftover_payload(self):
        for budget in (5, 30):
            result = h.blob_probe(self.root, budget, 3, time.monotonic()+5, payload=1024)
            self.assertEqual(set(result['phases_us']), set(h.PHASES))
            self.assertTrue(result['physical_calls_completed'])
            self.assertEqual(result['expiries'], 0)
            self.assertTrue(all(len(v) == 3 for v in result['phases_us'].values()))
        self.assertEqual(list(self.root.iterdir()), [])

    def test_probe_unsupported_budget_deadline_and_corrupt_read_refuse(self):
        with self.assertRaises(h.Refused): h.blob_probe(self.root, 0, 1, time.monotonic()+5)
        with self.assertRaises(h.Refused): h.blob_probe(self.root, 5, 1, time.monotonic()-1)
        with patch.object(h.os, 'read', return_value=b'wrong'), self.assertRaises(h.Refused):
            h.blob_probe(self.root, 5, 1, time.monotonic()+5, payload=4)
        self.assertTrue(any(self.root.iterdir()))

    def test_unexpected_existing_probe_file_is_never_overwritten(self):
        path=self.root/'probe-5-0';path.write_bytes(b'preserve exact fixture')
        with self.assertRaises(h.Refused):h.blob_probe(self.root,5,1,time.monotonic()+5,payload=4)
        self.assertEqual(path.read_bytes(),b'preserve exact fixture')

    def test_journal_constructor_failure_releases_owned_descriptors(self):
        before=len(list(Path('/proc/self/fd').iterdir()))
        with patch.object(h.Journal,'save',side_effect=OSError('synthetic no media')),self.assertRaises(OSError):h.Journal(self.root/'failed',{})
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())),before)
        self.assertTrue((self.root/'failed').exists())

    def test_readback_short_reads_are_completed(self):
        actual = os.read
        with patch.object(h.os, 'read', side_effect=lambda fd,n: actual(fd,min(n,7))):
            self.assertEqual(h.blob_probe(self.root, 5, 1, time.monotonic()+5, payload=99)['expiries'], 0)

    def test_private_json_symlink_hash_and_writeable_parent_refuse(self):
        path=self.root/'input.json';path.write_bytes(b'{}\n');path.chmod(0o600)
        self.assertEqual(h.private_bytes(path,h.sha(b'{}\n')),b'{}\n')
        with self.assertRaises(h.Refused):h.private_bytes(path,'0'*64)
        alias=self.root/'alias';alias.symlink_to(path)
        with self.assertRaises(h.Refused):h.private_bytes(alias)
        self.root.chmod(0o777)
        with self.assertRaises(h.Refused):h.private_bytes(path)

    def test_one_use_journal_no_overwrite_and_rebound_refuse(self):
        root=self.root/'operation';journal=h.Journal(root,{'synthetic_fixture':True})
        try:
            journal.save('proof.json',{'actual_tiny_fs':True})
            with self.assertRaises(FileExistsError):journal.save('proof.json',{})
            root.rename(self.root/'original');root.mkdir(mode=0o700)
            with self.assertRaises(h.Refused):journal.save('after.json',{})
        finally:journal.close()

    def test_qualify_cli_reads_private_inputs_but_creates_no_actor_or_evidence_root(self):
        operation=str(uuid.uuid4());value=inventory(operation)
        script=self.root/'hdd_benchmark.py';script.write_bytes(b'# synthetic private source\n');script.chmod(0o600)
        for name in ('interface.py','command_process.py'):
            raw=(SOURCE.parent/name).read_bytes();(self.root/name).write_bytes(raw);(self.root/name).chmod(0o600)
            value['process_sources'][name]=h.sha(raw)
        source_pin=h.sha(script.read_bytes());path=self.root/'inventory.json';path.write_bytes(h.canonical(value));path.chmod(0o600)
        argv=['hdd_benchmark.py','qualify','--inventory',str(path),'--inventory-sha256',h.sha(path.read_bytes()),'--source-sha256',source_pin,'--operation',operation]
        before=os.umask(0o077)
        try:
            with patch.object(h,'SELF',script),patch.object(h,'__file__',str(script)),patch.object(sys,'argv',argv),patch.object(sys,'dont_write_bytecode',True),patch.object(h,'PoolLock',side_effect=AssertionError('no run lock')),patch('subprocess.Popen',side_effect=AssertionError('no actors')),contextlib.redirect_stdout(io.StringIO()) as output:
                h.main()
            result=h.decode(output.getvalue().encode());self.assertFalse(result['effects_permitted'])
            self.assertFalse(Path(value['evidence_root']).exists())
        finally:os.umask(before)

    def test_fixed_container_arguments_no_pull_network_ports_or_anonymous_volume(self):
        operation=str(uuid.uuid4());value=inventory(operation);commands=[]
        cid='e'*64
        class Fake:
            closed=True
            def run(inner,binary,args,stdin=None,allowed_failure=False):
                commands.append((binary,args));return 0,(cid+'\n').encode(),b''
        bench=h.Benchmark(value,operation,None,Fake(),time.monotonic()+5)
        class J:
            def save(self,*args):return 'f'*64
        bench.journal=J();bench.journal.root=self.root;mount=self.root/'synthetic-data'
        observed={'Limits':{'readonly':True,'network':'none','cpu':1000000000,'cpuset':str(bench.cpu),'memory':536870912,'swap':536870912,'pids':128,'privileged':False,'capdrop':['ALL'],'ports':None,'restart':{'Name':'no','MaximumRetryCount':0}},'User':'70:70','Id':cid,'Image':value['native_pg_image']['image_id'],'State':{'Running':False,'Pid':0},'Labels':{'openlegal.hdd-benchmark.operation':operation},'Mounts':[{'Type':'bind','Source':str(mount/'pgdata'),'Destination':'/var/lib/postgresql','RW':True}]}
        def inspect(cid):
            args=commands[0][1];value=copy.deepcopy(observed);value['Entrypoint']=['/bin/sh'];value['Cmd']=args[args.index(value['Image'])+1:];value['Mounts'].append({'Type':'bind','Source':str(self.root/'gates'/'synthetic'),'Destination':'/openlegal-start','RW':False});return value
        with patch.object(bench,'check_capacity'),patch.object(bench,'inspect_container',side_effect=inspect):bench.create_container('synthetic',mount,['postgres','--version'])
        args=commands[0][1]
        for flag in ('--pull=never','--network=none','--read-only','--cpus=1','--memory=512m','--pids-limit=128','--cap-drop=ALL','--restart=no'):
            self.assertIn(flag,args)
        self.assertIn(value['native_pg_image']['image_id'],args)
        self.assertNotIn('-p',args);self.assertNotIn('--privileged',args)
        self.assertEqual(args[0],'create');self.assertNotIn('--host',args)
        # Native Docker rejected the prior bare ',rw' field with exit125.
        mounts=[args[i+1] for i,arg in enumerate(args) if arg=='--mount']
        self.assertIn('type=bind,src='+str(mount/'pgdata')+',dst=/var/lib/postgresql',mounts)
        self.assertTrue(all(all('=' in field or field=='readonly' for field in item.split(',')) for item in mounts))

    def test_cleanup_refuses_live_actors_and_changed_guids_before_destroy(self):
        operation=str(uuid.uuid4());value=inventory(operation)
        class Fake:
            closed=True
        bench=h.Benchmark(value,operation,None,Fake(),time.monotonic()+5)
        bench.containers={'e'*64:{'removed':False}}
        with patch.object(bench,'out',side_effect=AssertionError('no cleanup commands')),self.assertRaises(h.Refused):bench.cleanup()
        bench.containers={};fixed=bench.fixed;bench.datasets={fixed['dataset_parent']:'1',**{r['name']:str(i+2)for i,r in enumerate(fixed['datasets'])}}
        calls=[]
        def out(binary,args,stdin=None):
            calls.append(args)
            if binary=='docker':return b''
            return ('\n'.join(bench.datasets)+'\n').encode()
        with patch.object(bench,'out',side_effect=out),patch.object(bench,'properties',return_value={'guid':'wrong'}),self.assertRaises(h.Refused):bench.cleanup()
        self.assertFalse(any('destroy'in a for a in calls))


    def test_short_actor_gate_requires_actual_cgroup_before_any_payload_release(self):
        operation=str(uuid.uuid4());value=inventory(operation);cid='e'*64;events=[]
        gate=self.root/'gate';gate.mkdir(mode=0o755);meta=gate.stat()
        class J:
            def save(inner,name,body):
                events.append(name)
                if name.endswith('-running.json'):self.assertFalse((gate/'go').exists())
                return 'f'*64
        class Runner:
            closed=True
            def seal(inner):events.append('sealed')
        bench=h.Benchmark(value,operation,J(),Runner(),time.monotonic()+5);bench.gates['short']={'path':gate,'dev':meta.st_dev,'ino':meta.st_ino,'released':False};bench.containers[cid]={'label':'short','running_witness':None,'removed':False}
        def witness(pid):
            self.assertFalse((gate/'go').exists());events.append('actual-witness');return {'group':'/docker/'+cid,'path':'/synthetic/cgroup','dev':1,'ino':2,'pid':pid,'start_ticks':3}
        before=os.umask(0o077)
        try:
            with patch.object(bench,'check_capacity'),patch.object(bench,'out',return_value=(cid+'\n').encode()),patch.object(bench,'inspect_container',return_value={'State':{'Running':True,'Pid':123}}),patch.object(h,'cgroup_identity',side_effect=witness):bench.start_container(cid)
        finally:os.umask(before)
        self.assertEqual((gate/'go').read_text(),operation+':short\n');self.assertEqual(stat.S_IMODE((gate/'go').stat().st_mode),0o644);self.assertLess(events.index('actual-witness'),events.index('container-short-released.json'))
    def test_short_actor_unsupported_cgroup_and_missing_witness_never_pass_cleanup(self):
        operation=str(uuid.uuid4());value=inventory(operation);cid='e'*64;gate=self.root/'gate';gate.mkdir(mode=0o755);meta=gate.stat()
        class Runner:
            closed=True
        bench=h.Benchmark(value,operation,None,Runner(),time.monotonic()+5);bench.gates['short']={'path':gate,'dev':meta.st_dev,'ino':meta.st_ino,'released':False};bench.containers[cid]={'label':'short','running_witness':None,'removed':False}
        with patch.object(bench,'check_capacity'),patch.object(bench,'out',return_value=(cid+'\n').encode()),patch.object(bench,'inspect_container',return_value={'State':{'Running':True,'Pid':123}}),patch.object(h,'cgroup_identity',return_value={'group':'/unsupported'}),self.assertRaises(h.Refused):bench.start_container(cid)
        self.assertFalse((gate/'go').exists())
        with patch.object(bench,'inspect_container',return_value={'State':{'Running':False,'Pid':0,'ExitCode':0}}),self.assertRaises(h.Refused):bench.finish_container(cid)

class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.operation=str(uuid.uuid4());self.value=inventory(self.operation)
        class J:
            def save(self,*args):return 'a'*64
        self.bench=h.Benchmark(self.value,self.operation,J(),None,time.monotonic()+5)
    def out(self,free,available,used=None,quota='2147483648'):
        def read(binary,args,stdin=None):
            if binary=='zpool':return str(free).encode()
            if args[-1]==self.value['pool']['name']:return str(available).encode()
            return ('guid\t42\nused\t'+str(used)+'\nquota\t'+quota+'\norg.openlegal:hdd-benchmark\t'+self.operation+'\n').encode()
        return read
    def test_initial_two_gibibytes_plus_explicit_reserve_and_minimum_root_available(self):
        required=2147483648+4096
        with patch.object(self.bench,'out',side_effect=self.out(required,required)):self.bench.check_capacity('before-test')
        for free,available in((required-1,required),(required,required-1)):
            with patch.object(self.bench,'out',side_effect=self.out(free,available)),self.assertRaises(h.Refused):self.bench.check_capacity('before-test')
    def test_parent_remaining_quota_is_observed_and_wrong_quota_refuses(self):
        self.bench.datasets[self.bench.fixed['dataset_parent']]='42';required=1073741824+4096
        with patch.object(self.bench,'out',side_effect=self.out(required,required,1073741824)):self.bench.check_capacity('before-test')
        with patch.object(self.bench,'out',side_effect=self.out(required,required,1073741824,'0')),self.assertRaises(h.Refused):self.bench.check_capacity('before-test')
    def test_reserve_is_explicit_typed_authority_bound_not_an_adoption_threshold(self):
        for reserve in(0,-1,True):
            bad=copy.deepcopy(self.value);bad['pool']['reserve_bytes']=reserve
            with self.assertRaises(h.Refused):h.validate_inventory(bad,self.operation)
        self.assertEqual(h.plan(self.value,self.operation)['pool_reserve_bytes'],4096)

class ProcessOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='hdd-process-owned-',dir='/tmp');self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700);self.inventory=inventory(str(uuid.uuid4()))
        for name in ('interface.py','command_process.py'):
            raw=(SOURCE.parent/name).read_bytes();path=self.root/name;path.write_bytes(raw);path.chmod(0o600)
            self.inventory['process_sources'][name]=h.sha(raw)
        self.self_patch=patch.object(h,'SELF',self.root/'hdd_benchmark.py');self.self_patch.start();self.addCleanup(self.self_patch.stop)
        self.binary=Path(sys.executable).resolve()
        self.inventory['binaries']['zfs']={'path':str(self.binary),'sha256':h.sha(self.binary.read_bytes())}
        self.journal=h.Journal(self.root/'journal',{'synthetic_direct_runner_fixture':True});self.addCleanup(self.journal.close)
        self.runner=h.Runner(self.inventory,self.journal,time.monotonic()+10,self.journal.check)
    def result(self):
        return dict(stdout=b'',stderr=b'',actual_exit_code=17,expired=False,output_limit_exceeded=False,
            owned_processes_closed=True,unexpected_descendants=False,elapsed_seconds=.001,started=True,interrupted=False)
    def test_actual_bytes_and_nonzero_only_allowed_failure(self):
        rc,out,err=self.runner.run('zfs',['-I','-B','-c','import sys;print("synthetic");sys.exit(17)'],allowed_failure=True)
        self.assertEqual((rc,out,err),(17,b'synthetic\n',b''));self.assertTrue(self.runner.closed)
        record=h.decode((self.journal.root/'command-0001-exit.json').read_bytes())
        self.assertTrue(record['started']);self.assertTrue(record['owned_processes_closed']);self.assertFalse(record['unexpected_descendants'])
        with self.assertRaises(h.Refused):self.runner.run('zfs',['-I','-B','-c','import sys;sys.exit(19)'])
    def test_actual_setsid_descendant_refuses_zero_leader_even_allowed_failure(self):
        path=self.root/'escaped'
        code=('import os,time,pathlib;p=os.fork()\nif p==0:\n os.setsid();os.close(0);os.close(1);os.close(2);pathlib.Path('+repr(str(path))+').write_text(str(os.getpid()));time.sleep(30);os._exit(0)\nelse:time.sleep(.08);os._exit(0)')
        with self.assertRaisesRegex(h.Refused,'failed_or_unclosed'):self.runner.run('zfs',['-I','-B','-c',code],allowed_failure=True)
        self.assertTrue(self.runner.closed);self.assertFalse(Path('/proc/'+path.read_text()).exists())
        record=h.decode((self.journal.root/'command-0001-exit.json').read_bytes())
        self.assertEqual(record['actual_exit_code'],0);self.assertTrue(record['unexpected_descendants']);self.assertTrue(record['owned_processes_closed'])
    def test_dependency_drift_refuses_before_dispatch(self):
        dep=self.root/'command_process.py';dep.write_bytes(dep.read_bytes()+b'\n# changed\n')
        with patch.object(self.runner.process,'execute',side_effect=AssertionError('no actor')):
            with self.assertRaises(h.Refused):self.runner.run('zfs',['-c','pass'])
        self.assertFalse((self.journal.root/'command-0001-intent.json').exists())
    def test_both_dependencies_authenticate_before_any_source_compile(self):
        bad=copy.deepcopy(self.inventory);bad['process_sources']['command_process.py']='0'*64
        with patch('builtins.compile',side_effect=AssertionError('authenticate both first')):
            with self.assertRaises(h.Refused):h.load_process_sources(bad)
    def test_missing_extra_or_arbitrary_dependency_name_refuses(self):
        value=copy.deepcopy(self.inventory);value['process_sources']['other.py']='a'*64
        with self.assertRaises(h.Refused):h.process_source_bytes(value)
        value['process_sources']={'interface.py':'a'*64}
        with self.assertRaises(h.Refused):h.process_source_bytes(value)
    def test_allowed_failure_cannot_admit_other_actual_flags(self):
        for key in ('expired','output_limit_exceeded','unexpected_descendants','interrupted','owned_processes_closed','started'):
            value=self.result();value[key]=not value[key]
            with self.subTest(key=key),patch.object(self.runner.process,'execute',return_value=types.SimpleNamespace(**value)):
                with self.assertRaises(h.Refused):self.runner.run('zfs',['-c','pass'],allowed_failure=True)
    def test_original_deadline_and_held_hashed_executable_fd(self):
        result=types.SimpleNamespace(**dict(self.result(),actual_exit_code=0));called=[]
        def execute(command,**kw):
            called.append(kw);self.assertEqual(kw['deadline'],self.runner.deadline)
            actual=os.fstat(kw['executable_fd']);expected=self.binary.stat()
            self.assertEqual((actual.st_dev,actual.st_ino),(expected.st_dev,expected.st_ino));self.assertEqual(kw['max_output'],h.LIMIT)
            return result
        with patch.object(self.runner.process,'execute',side_effect=execute):self.runner.run('zfs',['-c','pass'])
        self.assertEqual(len(called),1)
    def test_dependency_loader_restores_previous_interface_module(self):
        prior=sys.modules.get('interface');h.load_process_sources(self.inventory)
        self.assertIs(sys.modules.get('interface'),prior)


class DockerTemporaryRootTests(unittest.TestCase):
    def setUp(self):
        ProcessOwnershipTests.setUp(self)
        self.operation=self.inventory['evidence_root'].removeprefix('/tmp/openlegal-hdd-benchmark-')
        self.data=self.root/'data';self.data.mkdir(mode=0o700)
        self.inventory['docker']['root_dir']=str(self.data)
        self.inventory['binaries']['docker']=self.inventory['binaries']['zfs']
        self.calls=[]
    def command(self,binary,args,stdin=None,allowed_failure=False):
        self.calls.append((binary,args,stdin,allowed_failure))
        return 0,(str(self.data)+'\n').encode() if 'info' in args else b'fixture-output',b''
    def test_required_typed_docker_schema_and_authority_plan(self):
        schema_fixture=inventory(self.operation);schema_fixture['docker']=self.inventory['docker']
        self.assertEqual(h.plan(schema_fixture,self.operation)['docker'],self.inventory['docker'])
        for change in ({'endpoint':'tcp://localhost:2375','root_dir':str(self.data)},
                       {'endpoint':'unix:///tmp/other.sock','root_dir':str(self.data)},
                       {'endpoint':'unix:///var/run/docker.sock','root_dir':'/var/lib/docker'},
                       {'endpoint':'unix:///var/run/docker.sock','root_dir':'/tmp/a/../b'},
                       {'endpoint':'unix:///tmp/openlegal-hdd-docker-'+self.operation+'/docker.sock','root_dir':str(self.data)},
                       dict(self.inventory['docker'],context='production')):
            with self.subTest(change=change),self.assertRaises(h.Refused):h.validate_docker(change,self.operation)
    def test_actual_non_tmp_daemon_refuses_before_actor_or_dataset_effect(self):
        def wrong(binary,args,*unused):
            self.calls.append((binary,args));return 0,b'/var/lib/docker\n',b''
        for binary,args in (('docker',['create','--name','synthetic']),('zfs',['create','synthetic'])):
            self.calls=[]
            with patch.object(self.runner,'_run_command',side_effect=wrong),self.assertRaisesRegex(h.Refused,'actual_root_mismatch'):
                self.runner.run(binary,args)
            self.assertEqual(len(self.calls),1);self.assertIn('info',self.calls[0][1])
    def test_default_root_exact_match_fixed_empty_config_and_no_ambient_env(self):
        observed=[]
        def execute(command,**kw):
            observed.append((command,kw));value=ProcessOwnershipTests.result(self)
            value.update(actual_exit_code=0,stdout=(str(self.data)+'\n').encode() if 'info' in command else b'ok')
            return types.SimpleNamespace(**value)
        with patch.dict(os.environ,{'HOME':'/tmp/hostile-home','DOCKER_HOST':'tcp://hostile','DOCKER_CONTEXT':'hostile','DOCKER_CONFIG':'/tmp/hostile','HTTP_PROXY':'http://hostile'}),patch.object(self.runner.process,'execute',side_effect=execute):
            self.runner.run('docker',['image','inspect','synthetic'])
        self.assertEqual(len(observed),2)
        prefix=['--config',str(self.journal.root/'docker-client'),'--host','unix:///var/run/docker.sock']
        for command,kw in observed:
            self.assertEqual(command[1:5],prefix);self.assertEqual(kw['env'],h.ENV)
            self.assertNotIn('HOME',kw['env']);self.assertEqual(kw['deadline'],self.runner.deadline)
        self.assertEqual(list((self.journal.root/'docker-client').iterdir()),[])
    def test_all_route_overrides_refuse_without_any_dispatch(self):
        for arg in ('--host','--host=unix:///tmp/a','-H','-Hunix:///tmp/a','--context','--context=x','--config','--config=/tmp/a'):
            with self.subTest(arg=arg),patch.object(self.runner,'_run_command',side_effect=AssertionError('no dispatch')),self.assertRaises(h.Refused):
                self.runner.run('docker',['image','inspect',arg])
        for arg in ('-c','-cother','--context=other','--host=other','--config=other'):
            with self.subTest(first=arg),patch.object(self.runner,'_run_command',side_effect=AssertionError('no dispatch')),self.assertRaises(h.Refused):
                self.runner.run('docker',[arg,'info'])
    def test_client_nonempty_or_rebound_refuses(self):
        with patch.object(self.runner,'_run_command',side_effect=self.command):self.runner.run('docker',['version'])
        config=self.journal.root/'docker-client';(config/'config.json').write_text('{}')
        with patch.object(self.runner,'_run_command',side_effect=AssertionError('no dispatch')),self.assertRaises(h.Refused):self.runner.run('docker',['version'])
        (config/'config.json').unlink();config.rename(self.journal.root/'detached-client');config.mkdir(mode=0o700)
        with patch.object(self.runner,'_run_command',side_effect=AssertionError('no dispatch')),self.assertRaises(h.Refused):self.runner.run('docker',['version'])
    def test_data_root_rebind_refuses(self):
        with patch.object(self.runner,'_run_command',side_effect=self.command):self.runner.run('docker',['version'])
        self.data.rename(self.root/'old-data');self.data.mkdir(mode=0o700)
        with patch.object(self.runner,'_run_command',side_effect=AssertionError('no dispatch')),self.assertRaises(h.Refused):self.runner.run('docker',['version'])
    def test_private_socket_parent_and_inode_are_bound(self):
        parent=Path('/tmp/openlegal-hdd-docker-'+self.operation);parent.mkdir(mode=0o700)
        self.addCleanup(lambda:__import__('shutil').rmtree(parent))
        self.data=parent/'data';self.data.mkdir(mode=0o700)
        self.inventory['docker']={'endpoint':'unix://'+str(parent/'docker.sock'),'root_dir':str(self.data)}
        sock=socket.socket(socket.AF_UNIX);self.addCleanup(sock.close);sock.bind(str(parent/'docker.sock'))
        with patch.object(self.runner,'_run_command',side_effect=self.command):self.runner.run('docker',['version'])
        parent.chmod(0o755)
        with patch.object(self.runner,'_run_command',side_effect=AssertionError('no dispatch')),self.assertRaises(h.Refused):self.runner.run('docker',['version'])
        parent.chmod(0o700);(parent/'docker.sock').rename(parent/'old.sock')
        replacement=socket.socket(socket.AF_UNIX);self.addCleanup(replacement.close);replacement.bind(str(parent/'docker.sock'))
        with patch.object(self.runner,'_run_command',side_effect=AssertionError('no dispatch')),self.assertRaises(h.Refused):self.runner.run('docker',['version'])
    def test_private_socket_symlink_and_actual_root_mismatch_refuse(self):
        parent=Path('/tmp/openlegal-hdd-docker-'+self.operation);parent.mkdir(mode=0o700)
        self.addCleanup(lambda:__import__('shutil').rmtree(parent))
        self.data=parent/'data';self.data.mkdir(mode=0o700)
        self.inventory['docker']={'endpoint':'unix://'+str(parent/'docker.sock'),'root_dir':str(self.data)}
        sock=socket.socket(socket.AF_UNIX);self.addCleanup(sock.close);sock.bind(str(parent/'actual.sock'))
        (parent/'docker.sock').symlink_to(parent/'actual.sock')
        with patch.object(self.runner,'_run_command',side_effect=AssertionError('no dispatch')),self.assertRaises(h.Refused):self.runner.run('docker',['version'])

if __name__=='__main__':unittest.main(verbosity=2)
