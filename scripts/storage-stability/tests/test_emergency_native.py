"""Real /tmp FDs and harmless OS receipts; qualified external ports are synthetic.

These tests perform no Docker, Kubernetes, SQL, mount, network or provider action.
The synthetic dual journal is NOT physical independent-media qualification.
"""
import copy
from pathlib import Path
import os
import platform
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import command_process
import emergency_backend as e
import emergency_native as n
import linux_backend as linux
from interface import Refused
from inventory import canonical, decode, digest
import test_emergency_backend as authority_fixture
from test_emergency_backend import uid

REAL_SCOPE = n.SurvivorNative._scope
REAL_RUN = linux.LinuxBackend._run
REAL_KERNEL_MOUNTINFO = n._kernel_mountinfo
REAL_FD_MOUNT_ID = n._fd_mount_id


@unittest.skipUnless(os.geteuid() == 0, 'root-private /tmp descriptors require root')
class NativeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = authority_fixture.RecoveryTests(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.op = self.fixture.op; self.subject = self.fixture.subject
        self.base = self.fixture.base; self.data = Path(self.subject['standby']['data_root']['path'])
        (self.data/'pg_wal').mkdir(mode=0o700)
        (self.data/'pg_tblspc').mkdir(mode=0o700)
        os.chown(self.data/'pg_wal', 70, 70)
        old = Path.cwd(); os.chdir(self.data); self.addCleanup(os.chdir, old)
        self.backend = linux.LinuxBackend(); self.native = n.SurvivorNative(self.backend)
        self.addCleanup(self.native.close)
        self.position = copy.deepcopy(self.fixture.position)
        self.snapshot = copy.deepcopy(self.fixture.snapshot)
        self.closure = {'sessions': []}; self.identity = {
            'database': 'openlegal', 'session_user': 'survivor_reader', 'current_user': 'survivor_reader',
            'role': {'login': True, 'superuser': False, 'create_role': False, 'create_db': False,
                     'replication': False, 'bypass_rls': False, 'inherit': False},
            'broad_roles': False, 'owner_membership': False, 'function_owners_qualified': True}
        self.calls = []; self.mutate_receipt = None; self.drop_receipt = False
        self.raw_override=None;self.receipt_transform=None
        pg = self.op.inventory['linux']['postgres']
        pg.update(uid=70, admin_user='survivor_reader', database='openlegal')
        pg['standby']['mounts'] = [{'source': str(self.data), 'destination': '/pg/data',
                                   'rw': True, 'type': 'bind'}]
        self.inspect = {'Id': self.subject['standby']['container_id'],
            'Image': self.subject['standby']['image_id'],
            'State': {'Running': True, 'Pid': os.getpid(), 'StartedAt': 'synthetic-docker-start'},
            'Mounts': [{'Source': str(self.data), 'Destination': '/pg/data', 'RW': True, 'Type': 'bind'}]}
        # Only synthetic scope bypasses source installation/authority setup.
        # Native descriptor/proc and command receipt checks below remain real.
        self.addCleanup(patch.stopall)
        patch.object(n.SurvivorNative, '_scope', lambda adapter, op: self.subject).start()
        patch.object(linux.LinuxBackend, '_container_directory', staticmethod(self.container_fd)).start()
        patch.object(linux.LinuxBackend, '_run', self.dispatch).start()
        self.host_extra_mounts=[];self.native_extra_mounts=[]
        # The qualified container namespace is synthetic, just as Docker is;
        # host mountinfo/fdinfo and all opened /tmp inodes remain actual.
        patch.object(n.observation,'_kernel_mountinfo',self.kernel_view).start()
        cfg = self.base/'docker-client'; cfg.mkdir(mode=0o700)
        info = cfg.stat(); self.backend._docker_config = (cfg, (info.st_dev, info.st_ino))

    def container_fd(self, pid, path):
        self.assertEqual(pid, self.subject['standby']['process']['pid'])
        self.assertIn(path, ('/pg/data', '/pg/data/pg_wal'))
        return n._directory(self.data if path == '/pg/data' else self.data/'pg_wal')

    def kernel_view(self,pid,deadline):
        if pid is None:return REAL_KERNEL_MOUNTINFO(None,deadline)+'\n'.join(self.host_extra_mounts)
        self.assertEqual(pid,self.subject['standby']['process']['pid'])
        fd=os.open(self.data,os.O_RDONLY|os.O_DIRECTORY)
        try:mid=REAL_FD_MOUNT_ID(fd);dev=os.fstat(fd).st_dev
        finally:os.close(fd)
        row=f'{mid} 1 {os.major(dev)}:{os.minor(dev)} / /pg/data rw - ext4 synthetic-native rw\n'
        return row+'\n'.join(self.native_extra_mounts)

    def nested_mount(self,path,*,foreign=False):
        dev=self.data.stat().st_dev
        device='254:254' if foreign else f'{os.major(dev)}:{os.minor(dev)}'
        return f'2000000000 1 {device} / {path} rw - ext4 synthetic-nested rw\n'

    def dispatch(self, backend, op, binary, args, **kwargs):
        self.calls.append((binary, copy.deepcopy(args), kwargs))
        n._check(kwargs['deadline'])
        if binary == 'docker' and args[:1] == ['inspect']:
            self.assertEqual(args, ['inspect', '--format', n.INSPECT, self.subject['standby']['container_id']])
            return canonical(self.inspect)
        if binary == 'docker' and args[:2] == ['image', 'inspect']:
            self.assertIn(args[-1], (self.op.inventory['linux']['serving_image'], self.image['Id']))
            return canonical(self.image)
        if binary == 'kubectl':
            ix = args.index('get'); kind, name = args[ix+1:ix+3]
            self.assertEqual(args[-2:], ['-o', 'json'])
            return canonical(self.resources[(kind, name)])
        self.assertEqual(binary, 'docker'); self.assertEqual(args[0], 'exec')
        self.assertEqual(args[args.index('/usr/bin/env')-1], self.subject['standby']['container_id'])
        script = kwargs['stdin'].decode()
        if e.POSITION_SQL[7:] in script: result = self.position
        elif e.SNAPSHOT_SQL[7:] in script: result = self.snapshot
        elif e.CLOSURE_SQL[7:] in script: result = self.closure
        else: raise AssertionError('unexpected SQL')
        raw = self.raw_override if self.raw_override is not None else canonical({'identity': self.identity, 'result': result})
        backend._sequence += 1
        if not self.drop_receipt:
            dispatch = ['--config', str(backend._docker_config[0]), '--host', 'unix:///var/run/docker.sock']
            receipt = {'binary': binary, 'arguments_sha256': digest(canonical(dispatch+args)),
                'stdin_sha256': digest(kwargs['stdin']), 'stdout_sha256': digest(raw),
                'stdout_bytes': len(raw), 'stdout_hex': raw.hex(), 'actual_exit_code': 0,
                'started': True, 'owned_processes_closed': True, 'expired': False, 'interrupted': False,
                'output_limit_exceeded': False, 'unexpected_descendants': False}
            if self.mutate_receipt: self.mutate_receipt(receipt)
            backend._artifact(op, 'command-'+str(backend._sequence).zfill(4), receipt)
            if self.receipt_transform:
                name=op.phase+'-command-'+str(backend._sequence).zfill(4)+'.json'
                op.journal.values[name]=self.receipt_transform(op.journal.values[name])
        return raw

    def observe(self, deadline=None):
        return self.native._emergency_survivor_observe(self.op,
            container_id=self.subject['standby']['container_id'],
            deadline=deadline if deadline is not None else time.monotonic()+15)

    def test_common_healthy_metadata_reads_need_no_emergency_certificate(self):
        common=n.observation.NativeObservation(self.backend)
        self.addCleanup(common.close)
        with patch.object(e,'validate_authority',side_effect=AssertionError('healthy read consulted emergency certificate')):
            value=common._query_metadata(self.op,self.subject,'snapshot',time.monotonic()+10)
        self.assertEqual(value['result'],self.snapshot)
        reference=value['actual_command']
        raw=self.op.journal.read_phase_bytes(reference['phase'],reference['artifact'],reference['sha256'])
        self.assertEqual(decode(raw)['actual_exit_code'],0)
        self.assertNotIn('verify_latest-emergency-snapshot.json',self.op.journal.values)
        with self.assertRaisesRegex(Refused,'fixed_pg_selection'):
            common._query_metadata(self.op,self.subject,'snapshot',time.monotonic()+10,which='certificate-selected')
        count=len(self.calls)
        for kind in ('position','snapshot'):
            with self.subTest(kind=kind),self.assertRaisesRegex(Refused,'survivor_query_requires_standby'):
                common._query_metadata(self.op,self.subject,kind,time.monotonic()+10,which='primary')
        self.assertEqual(len(self.calls),count)

    def test_common_configuration_returns_fresh_projection_without_expected_certificate_hash(self):
        self.resources_fixture()
        common=n.observation.NativeObservation(self.backend);self.addCleanup(common.close)
        self.subject['runtime']['configuration_sha256']='0'*64
        with patch.object(e,'validate_authority',side_effect=AssertionError('healthy configuration consulted certificate')):
            value=common._configuration_projection(self.op,self.subject,time.monotonic()+10)
        self.assertEqual(value['configuration_sha256'],n.configuration_hash_v1(value['projection']))
        self.assertNotEqual(value['configuration_sha256'],self.subject['runtime']['configuration_sha256'])
        self.assertEqual(set(value['projection']['blob_survivors']),set(n.observation.SSD_SURVIVORS))

    def test_real_host_native_wal_and_proc_descriptors_stay_bound(self):
        first = self.native._native(self.op, self.subject, time.monotonic()+3)
        self.assertEqual(first, self.native._native(self.op, self.subject, time.monotonic()+3))
        self.assertEqual(len(self.native.anchors), 1)
        self.assertEqual(first['data_root'], self.subject['standby']['data_root'])
        held = next(iter(self.native.anchors.values()))['fds'][:]
        self.native.close()
        for fd in held:
            with self.assertRaises(OSError): os.fstat(fd)

    def test_real_replaced_selected_root_refuses(self):
        self.native._native(self.op, self.subject, time.monotonic()+3)
        self.data.rename(self.data.with_name('retained-original'))
        self.data.mkdir(mode=0o700); os.chown(self.data, 70, 70)
        with self.assertRaisesRegex(Refused, 'root_identity'):
            self.native._native(self.op, self.subject, time.monotonic()+3)

    def test_native_fd_wrong_inode_refuses(self):
        with patch.object(linux.LinuxBackend, '_container_directory',
                          staticmethod(lambda pid, path: n._directory(self.base))):
            with self.assertRaisesRegex(Refused, 'native_fd_correspondence'):
                self.native._native(self.op, self.subject, time.monotonic()+3)

    def test_native_process_start_boot_and_cid_are_exact(self):
        for field, value in (('start_ticks', '0'), ('boot_id', uid()), ('pid', os.getpid()+10000000)):
            original = self.subject['standby']['process'][field]
            self.subject['standby']['process'][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(Refused, 'lifetime'):
                self.native._native(self.op, self.subject, time.monotonic()+3)
            self.subject['standby']['process'][field] = original
        self.inspect['Id'] = self.subject['primary']['container_id']
        with self.assertRaisesRegex(Refused, 'native_identity'):
            self.native._native(self.op, self.subject, time.monotonic()+3)

    def test_external_wal_symlink_refuses_without_opening_target(self):
        (self.data/'pg_wal').rmdir(); (self.data/'pg_wal').symlink_to('/definitely-unavailable-primary-wal')
        with self.assertRaisesRegex(Refused, 'external_wal_unsupported'):
            self.native._native(self.op, self.subject, time.monotonic()+3)

    def test_same_device_or_foreign_nested_bind_mounts_refuse_in_both_namespaces(self):
        for native in (False,True):
            for foreign in (False,True):
                for tail in ('pg_wal','base/hidden'):
                    path=Path('/pg/data')/tail if native else self.data/tail
                    rows=self.native_extra_mounts if native else self.host_extra_mounts
                    rows.append(self.nested_mount(path,foreign=foreign))
                    with self.subTest(native=native,foreign=foreign,tail=tail),\
                         self.assertRaisesRegex(Refused,'nested_data_mount_unsupported'):
                        self.native._native(self.op,self.subject,time.monotonic()+3)
                    rows.clear()

    def test_wal_different_mount_id_refuses_even_with_same_device_and_inode_mapping(self):
        def kernel_id(fd):
            actual=REAL_FD_MOUNT_ID(fd)
            return actual+100000000 if os.readlink('/proc/self/fd/'+str(fd)).endswith('/pg_wal') else actual
        with patch.object(n.observation,'_fd_mount_id',side_effect=kernel_id):
            with self.assertRaisesRegex(Refused,'inline_wal_mount_changed'):
                self.native._native(self.op,self.subject,time.monotonic()+3)

    def test_empty_tablespace_directory_is_required_and_links_are_not_followed(self):
        spaces=self.data/'pg_tblspc';link=spaces/'foreign'
        link.symlink_to('/unavailable-original-tablespace')
        with self.assertRaisesRegex(Refused,'external_tablespace_unsupported'):
            self.native._native(self.op,self.subject,time.monotonic()+3)
        link.unlink();spaces.rmdir();spaces.symlink_to('/unavailable-original-tablespace')
        with self.assertRaisesRegex(Refused,'external_tablespace_unsupported'):
            self.native._native(self.op,self.subject,time.monotonic()+3)

    def test_late_native_nested_mount_is_rechecked_before_return(self):
        original=self.kernel_view;reads=0
        def late(pid,end):
            nonlocal reads
            if pid is not None:
                reads+=1
                if reads==2:self.native_extra_mounts.append(self.nested_mount('/pg/data/pg_wal/late'))
            return original(pid,end)
        with patch.object(n.observation,'_kernel_mountinfo',side_effect=late):
            with self.assertRaisesRegex(Refused,'nested_data_mount_unsupported'):
                self.native._native(self.op,self.subject,time.monotonic()+3)

    def test_held_anchor_mount_identity_is_checked_not_only_inode(self):
        self.native._native(self.op,self.subject,time.monotonic()+3)
        held=next(iter(self.native.anchors.values()))['fds'][0]
        with patch.object(n.observation,'_fd_mount_id',side_effect=lambda fd:REAL_FD_MOUNT_ID(fd)+(1 if fd==held else 0)):
            with self.assertRaisesRegex(Refused,'anchor_changed'):
                self.native._native(self.op,self.subject,time.monotonic()+3)

    def test_host_and_foreign_namespace_ids_are_validated_separately(self):
        fds=[n._directory(path) for path in (self.data,self.data/'pg_wal',self.data,self.data/'pg_wal')]
        foreign=2000000000;native_fds=set(fds[2:]);dev=os.fstat(fds[0]).st_dev
        def mounted_id(fd):return foreign if fd in native_fds else REAL_FD_MOUNT_ID(fd)
        def fresh_native(pid,path):
            fd=n._directory(self.data if path=='/pg/data' else self.data/'pg_wal')
            native_fds.add(fd);return fd
        def view(pid,end):
            if pid is None:return REAL_KERNEL_MOUNTINFO(None,end)
            return f'{foreign} 1 {os.major(dev)}:{os.minor(dev)} / /pg/data rw - ext4 synthetic-native rw\n'
        try:
            with patch.object(n.observation,'_fd_mount_id',side_effect=mounted_id),\
                 patch.object(n.observation,'_kernel_mountinfo',side_effect=view),\
                 patch.object(linux.LinuxBackend,'_container_directory',staticmethod(fresh_native)):
                layout=n._inline_layout(os.getpid(),'/pg/data',self.data,*fds,time.monotonic()+3)
            self.assertNotEqual(layout['host']['mount_id'],layout['native']['mount_id'])
            self.assertEqual(layout['native']['mount_id'],foreign)
        finally:
            for fd in fds:os.close(fd)

    def test_same_inode_fresh_path_changed_mount_id_refuses_and_partial_open_closes(self):
        fds=[n._directory(path) for path in (self.data,self.data/'pg_wal',self.data,self.data/'pg_wal')]
        opened=[];directory=n._directory
        def fresh(path):
            fd=directory(path);opened.append(fd);return fd
        def changed(fd):return REAL_FD_MOUNT_ID(fd)+(1 if fd in opened else 0)
        try:
            with patch.object(n.observation,'_directory',side_effect=fresh),patch.object(n.observation,'_fd_mount_id',side_effect=changed):
                with self.assertRaisesRegex(Refused,'mount_path_rebound'):
                    n._inline_layout(os.getpid(),'/pg/data',self.data,*fds,time.monotonic()+3)
            for fd in opened:
                with self.assertRaises(OSError):os.fstat(fd)
            opened.clear()
            def fail_second(path):
                if opened:raise OSError('synthetic second fresh open failed')
                return fresh(path)
            with patch.object(n.observation,'_directory',side_effect=fail_second):
                with self.assertRaises(OSError):
                    n._inline_layout(os.getpid(),'/pg/data',self.data,*fds,time.monotonic()+3)
            self.assertEqual(len(opened),1)
            with self.assertRaises(OSError):os.fstat(opened[0])
        finally:
            for fd in fds:os.close(fd)

    def test_observe_fixed_readonly_sql_uses_only_existing_named_role_and_unsets_service(self):
        deadline = time.monotonic()+15; ref = self.observe(deadline)
        self.assertEqual(e.read_evidence(self.op, ref, 'position')['result'], self.position)
        scripts = [(args, kw) for binary,args,kw in self.calls if args[:1] == ['exec']]
        self.assertEqual(len(scripts), 2)
        for args,kw in scripts:
            self.assertEqual(kw['deadline'], deadline)
            self.assertIn(b'BEGIN READ ONLY;', kw['stdin']); self.assertTrue(kw['stdin'].endswith(b'ROLLBACK;\n'))
            self.assertIn(b"statement_timeout='2000ms'", kw['stdin'])
            self.assertIn(b"transaction_timeout='5000ms'", kw['stdin'])
            self.assertIn(b"lock_timeout='1000ms'", kw['stdin'])
            self.assertEqual(args[args.index('/usr/bin/env')+1:args.index('psql')],
                             [x for name in n.UNSET for x in ('-u', name)])
            self.assertIn('PGPASSFILE=/dev/null', args); self.assertNotIn('PGSERVICE=', args)
            self.assertEqual(args[-4:], ['-U', 'survivor_reader', '-d', 'openlegal'])
            self.assertNotIn(self.subject['primary']['container_id'], args)
        self.assertNotIn('Config.Env', n.INSPECT)
        self.assertNotIn('json .State}',n.INSPECT)
        self.assertNotIn('Health',n.INSPECT);self.assertNotIn('Error',n.INSPECT)

    def test_primary_target_and_expired_original_deadline_never_query(self):
        with self.assertRaisesRegex(Refused, 'standby_only'):
            self.native._emergency_survivor_observe(self.op,
                container_id=self.subject['primary']['container_id'], deadline=time.monotonic()+1)
        with self.assertRaisesRegex(Refused, 'original_deadline'): self.observe(time.monotonic()-1)
        self.assertEqual(self.calls, [])

    def test_every_entry_requires_installed_private_source_pins(self):
        install=self.base/'install';install.mkdir(mode=0o700)
        source=Path(n.__file__).resolve().parent
        names=('emergency_native.py','emergency_backend.py','epoch_observation.py','linux_backend.py','interface.py','inventory.py')
        graph={}
        for name in names:
            raw=(source/name).read_bytes();path=install/name;path.write_bytes(raw);path.chmod(0o600)
            graph[name]=digest(raw)
        self.op.inventory['source_graph']=graph
        with patch.object(n,'__file__',str(install/'emergency_native.py')):
            self.assertEqual(REAL_SCOPE(self.native,self.op)['standby'],self.subject['standby'])
            (install/'emergency_native.py').write_bytes(b'changed after source pin\n')
            with self.assertRaisesRegex(Refused,'private_raw_hash'):REAL_SCOPE(self.native,self.op)
        self.op.inventory['source_graph']={}
        with self.assertRaisesRegex(Refused,'source_graph'):REAL_SCOPE(self.native,self.op)

    def test_late_dual_journal_ack_is_diagnostic_and_cannot_return_success(self):
        original=self.op.journal.save
        def late(name,value):
            result=original(name,value)
            if '-emergency-position-' in name:time.sleep(.06)
            return result
        self.op.journal.save=late
        with self.assertRaisesRegex(Refused,'original_deadline'):self.observe(time.monotonic()+.04)
        self.assertIn('verify_latest-emergency-position-0001.json',self.op.journal.values)
        self.assertFalse(any(e.CLOSURE_SQL[7:].encode() in kw.get('stdin',b'') for _,_,kw in self.calls))

    def test_raw_os_receipt_required_and_every_success_flag_is_exact(self):
        self.drop_receipt = True
        with self.assertRaisesRegex(Refused, 'actual_command_missing'): self.observe()
        self.drop_receipt = False
        for key,bad in (('actual_exit_code', True), ('actual_exit_code', 7), ('started', False),
                        ('owned_processes_closed', False), ('stdout_hex', ''), ('arguments_sha256', '0'*64),
                        ('stdin_sha256', '0'*64), ('expired', True), ('unexpected_descendants', True)):
            self.mutate_receipt = lambda row, k=key, v=bad: row.update({k:v})
            with self.subTest(key=key,bad=bad), self.assertRaisesRegex(Refused, 'not_witnessed'): self.observe()

    def test_actual_role_privileges_and_memberships_are_refused(self):
        for where,key,value in (('role','superuser',True), ('role','inherit',True),
                                ('role','login',False), (None,'owner_membership',True),
                                (None,'broad_roles',True), (None,'current_user','other'),
                                (None,'function_owners_qualified',False)):
            target = self.identity[where] if where else self.identity; old = target[key]; target[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(Refused, 'observer_role'): self.observe()
            target[key] = old

    def test_nonempty_sql_sessions_do_not_hide_a_different_application(self):
        self.closure = {'sessions':[{'pid':987654,'backend_start':'synthetic','state':None}]}
        with self.assertRaisesRegex(Refused, 'original_deadline'): self.observe(time.monotonic()+.07)
        scripts = [kw['stdin'] for _,args,kw in self.calls if args[:1] == ['exec']]
        self.assertTrue(any(e.CLOSURE_SQL[7:].encode() in script for script in scripts))
        self.assertFalse(any(b'pg_terminate_backend' in script or b'pg_cancel_backend' in script for script in scripts))

    def test_receiver_reconnect_or_receive_change_refuses_after_target(self):
        self.observe()
        self.position['wal_receiver_count'] = 1
        with self.assertRaisesRegex(Refused, 'receiver_changed_after_fixed_end'): self.observe()
        self.position['wal_receiver_count'] = 0; self.position['receive_lsn'] = '0/200'
        with self.assertRaisesRegex(Refused, 'receiver_changed_after_fixed_end'): self.observe()

    def test_snapshot_fixed_rr_limits_and_both_reconnect_boundaries(self):
        self.observe()
        for boundary in ('position_before', 'position_after'):
            self.snapshot[boundary]['wal_receiver_count'] = 1
            # Reset only the synthetic in-memory slot to isolate each negative;
            # the production O_EXCL dual-journal implementation is unchanged.
            self.op.journal.values.pop('verify_latest-emergency-snapshot.json', None)
            with self.subTest(boundary=boundary), self.assertRaisesRegex(Refused, 'receiver_changed_after_fixed_end'):
                self.native._emergency_survivor_snapshot(self.op, container_id=self.subject['standby']['container_id'])
            self.snapshot[boundary]['wal_receiver_count'] = 0
        self.op.journal.values.pop('verify_latest-emergency-snapshot.json', None)
        ref = self.native._emergency_survivor_snapshot(self.op, container_id=self.subject['standby']['container_id'])
        self.assertEqual(e.read_evidence(self.op, ref, 'snapshot')['result'], self.snapshot)
        scripts = [kw for _,args,kw in self.calls if args[:1] == ['exec'] and e.SNAPSHOT_SQL[7:].encode() in kw['stdin']]
        for kw in scripts:
            self.assertIn(b'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;', kw['stdin'])
            for setting in (b"statement_timeout='30000ms'", b"transaction_timeout='35000ms'",
                            b"lock_timeout='1000ms'", b"idle_in_transaction_session_timeout='5000ms'"):
                self.assertIn(setting, kw['stdin'])
            self.assertEqual(kw['timeout'], 36)

    def test_bulk_snapshot_reuses_strict_nonfinite_duplicate_depth_and_output_bounds(self):
        invalid=((b'{"result":NaN}', 'metadata_nonfinite'),
                 (b'{"result":{},"result":{}}','metadata_duplicate'),
                 (b'['*34+b'0'+b']'*34,'metadata_structure_bound'),
                 (b' '*(linux.MAX_OUTPUT+1),'sql_output_bound'))
        for raw,code in invalid:
            self.raw_override=raw
            with self.subTest(code=code),self.assertRaisesRegex(Refused,code):
                self.native._sql(self.op,self.subject,'snapshot',time.monotonic()+5)
        self.assertNotIn('verify_latest-emergency-snapshot.json',self.op.journal.values)

    def test_bulk_receipt_keeps_duplicate_nonfinite_and_depth_checks(self):
        for transform,code in (
            (lambda raw:raw[:-2]+b',"actual_exit_code":0}\n','metadata_duplicate'),
            (lambda raw:raw[:-2]+b',"unexpected":NaN}\n','metadata_nonfinite'),
            (lambda raw:b'['*34+b'0'+b']'*34,'metadata_structure_bound')):
            self.receipt_transform=transform
            with self.subTest(code=code),self.assertRaisesRegex(Refused,code):
                self.native._sql(self.op,self.subject,'snapshot',time.monotonic()+5)

    def test_position_and_closure_keep_small_receipt_and_result_caps(self):
        self.raw_override=b' '*(4*1024**2+1)
        for kind in ('position','sql-closure'):
            with self.subTest(kind=kind),self.assertRaisesRegex(Refused,'json_bound'):
                self.native._sql(self.op,self.subject,kind,time.monotonic()+5)

    def resources_fixture(self):
        config = self.op.inventory['linux']; namespace = 'openlegal'
        ref = 'ghcr.io/publicdata-stream/openlegal-server@sha256:'+'9'*64
        config.update(namespace=namespace, context='fixed', serving_image=ref,serving_workload='serve',
                      serving_runtime_image_id=self.subject['runtime']['image_id'])
        ingestion='ghcr.io/publicdata-stream/openlegal-server-ingestion@sha256:'+'8'*64
        self.image = {'Id':config['serving_runtime_image_id'], 'Os':'linux',
            'Architecture':{'x86_64':'amd64','aarch64':'arm64'}[platform.machine()],
            'Labels':{'org.opencontainers.image.revision':self.subject['runtime']['revision']}}
        configfile = self.base/'kubeconfig'; configfile.write_bytes(b'synthetic-private-config'); configfile.chmod(0o600)
        config['kubeconfig'] = {'path':str(configfile),'sha256':digest(configfile.read_bytes())}
        # secure_path normally rejects /tmp ancestors. Only that external path
        # policy is synthetic here; marker/root FDs below are actual /tmp data.
        patch.object(linux, 'secure_path', lambda path: None).start()
        self.resources = {}
        def resource(kind,name,spec=None,data=None,cluster=False):
            item={'metadata':{'name':name,'uid':uid()},'kind':kind}
            if not cluster:item['metadata']['namespace']=namespace
            if spec is not None:item['spec']=spec
            if data is not None:item['data']=data
            self.resources[(kind,name)]=item;return item
        pod={'containers':[{'name':'server','image':ref,'volumeMounts':[]}], 'volumes':[]}
        config['persistent_volumes']=[]; config['bound_roots']=[]
        for role,root in self.subject['blob_roots'].items():
            config['bound_roots'].append(dict(role=role,**root))
            if role in n.observation.HDD_LOST:continue
            pv=resource('persistentvolume',role.replace('_','-'),{'local':{'path':root['path']},
                        'nodeAffinity':{'required':'fixed'}},cluster=True)
            config['persistent_volumes'].append({'name':pv['metadata']['name'],'uid':pv['metadata']['uid'],'path':root['path']})
            claim=resource('persistentvolumeclaim',role.replace('_','-'),{'volumeName':pv['metadata']['name'],'accessModes':['ReadWriteOnce']})
            pod['volumes'].append({'name':role,'persistentVolumeClaim':{'claimName':claim['metadata']['name']}})
            pod['containers'][0]['volumeMounts'].append({'name':role,'mountPath':root['container_path']})
        config['workloads']=[]
        for name in ('serve','scheduler'):
            item=resource('deployment',name,{'replicas':0,'selector':{'matchLabels':{'app':name}},
                'template':{'metadata':{'labels':{'app':name}},'spec':copy.deepcopy(pod)}})
            if name=='scheduler':item['spec']['template']['spec']['containers'][0]['image']=ingestion
            config['workloads'].append({'name':name,'uid':item['metadata']['uid'],
                'pod_spec_sha256':digest(canonical(item['spec']['template']['spec']))})
        job=resource('job','collector',{'suspend':True,'template':{'spec':copy.deepcopy(pod)},'backoffLimit':0})
        job['spec']['template']['spec']['containers'][0]['image']=ingestion
        config['jobs']=[{'name':'collector','uid':job['metadata']['uid']}]
        cm=resource('configmap','runtime',data={'fixed-template':'synthetic-config'})
        config['config_maps']=[{'name':'runtime','uid':cm['metadata']['uid'],'data_sha256':digest(canonical(cm['data']))}]
        service=resource('service','serve',{'selector':{'app':'serve'},'ports':[{'port':80}]})
        config['serving_service']={'name':'serve','uid':service['metadata']['uid'],'spec_sha256':digest(canonical(service['spec']))}

    def bind_synthetic_profile(self):
        # The qualified future producer supplies this pre-ACK hash. Here we
        # independently retain the freshly constructed projection on refusal;
        # no actual certificate/pre-ACK artifact is produced or modified.
        with patch.object(n.observation,'configuration_hash_v1',wraps=n.configuration_hash_v1) as hashed:
            with self.assertRaisesRegex(Refused,'profile_v2_mismatch'):
                self.native._emergency_current_configuration(self.op)
            self.projection=copy.deepcopy(hashed.call_args.args[0])
        self.subject['runtime']['configuration_sha256']=n.configuration_hash_v1(self.projection)

    def test_config_observes_fresh_image_kube_and_two_surviving_markers_only(self):
        self.resources_fixture(); self.bind_synthetic_profile()
        ref=self.native._emergency_current_configuration(self.op)
        value=e.read_evidence(self.op,ref,'configuration')
        self.assertEqual(value['runtime_revision'],self.image['Labels']['org.opencontainers.image.revision'])
        self.assertEqual(value['runtime_image_id'],self.image['Id'])
        self.assertEqual(value['blob_binding_sha256'],self.position['control']['blob_binding_sha256'])
        self.assertEqual(set(self.projection['blob_survivors']),set(n.observation.SSD_SURVIVORS))
        self.assertEqual(n.configuration_hash_v1(self.projection),digest(n.DOMAIN+canonical(self.projection)))
        for role in n.observation.HDD_LOST:
            self.assertFalse(Path(self.subject['blob_roots'][role]['path']).exists())
        self.assertFalse(Path(self.subject['primary']['data_root']['path']).exists())
        self.assertFalse(any(self.subject['primary']['container_id'] in args for _,args,_ in self.calls))

    def test_config_normalizes_only_documented_fence_fields(self):
        self.resources_fixture(); self.bind_synthetic_profile()
        self.resources[('deployment','serve')]['spec']['replicas']=17
        self.resources[('deployment','serve')]['metadata'].update(resourceVersion='new',annotations={'fence':'new'})
        self.resources[('deployment','serve')]['status']={'readyReplicas':0}
        self.resources[('job','collector')]['spec']['suspend']=False
        self.native._emergency_current_configuration(self.op)
        self.resources[('persistentvolume','cache-replica')]['spec']['nodeAffinity']={'required':'changed'}
        with self.assertRaisesRegex(Refused,'profile_v2_mismatch'):
            self.native._emergency_current_configuration(self.op)

    def test_profile_is_closed_at_root_resource_and_domain_boundaries(self):
        self.resources_fixture();self.bind_synthetic_profile()
        for mutation in ('root-extra','bad-resource-pin','duplicate-resource','wrong-domain-version'):
            profile=copy.deepcopy(self.projection)
            if mutation=='root-extra':profile['blob_survivors']['cache_replica']['unobserved']='invented'
            if mutation=='bad-resource-pin':profile['deployments'][0]['spec_sha256']='not-a-pin'
            if mutation=='duplicate-resource':profile['deployments'].append(copy.deepcopy(profile['deployments'][0]))
            if mutation=='wrong-domain-version':profile['schema_version']=1
            with self.subTest(mutation=mutation),self.assertRaises(Refused):n.configuration_hash_v1(profile)

    def test_config_changed_native_image_label_pvc_or_marker_cannot_copy_certificate(self):
        self.resources_fixture(); self.bind_synthetic_profile()
        original=self.image['Labels']['org.opencontainers.image.revision']
        self.image['Labels']['org.opencontainers.image.revision']='1'*40
        with self.assertRaisesRegex(Refused,'profile_v2_mismatch'):
            self.native._emergency_current_configuration(self.op)
        self.image['Labels']['org.opencontainers.image.revision']=original
        self.resources[('persistentvolumeclaim','cache-replica')]['spec']['volumeName']='unselected'
        with self.assertRaisesRegex(Refused,'unbound_pvc'):
            self.native._emergency_current_configuration(self.op)
        self.resources[('persistentvolumeclaim','cache-replica')]['spec']['volumeName']='cache-replica'
        marker=Path(self.subject['blob_roots']['cache_replica']['path'])/'.openlegal-seed-v1.json'
        marker.write_bytes(marker.read_bytes()+b' ')
        with self.assertRaises(Refused):self.native._emergency_current_configuration(self.op)

    def test_init_container_reference_must_be_an_immutable_project_artifact(self):
        self.resources_fixture()
        item=self.resources[('job','collector')]
        item['spec']['template']['spec']['initContainers']=[{'name':'unqualified','image':'mutable:latest'}]
        with self.assertRaisesRegex(Refused,'template_image'):
            self.native._emergency_current_configuration(self.op)

    def test_distinct_ingestion_artifact_is_accepted_but_changed_or_mutable_ref_refuses(self):
        self.resources_fixture();self.bind_synthetic_profile()
        server=self.op.inventory['linux']['serving_image']
        scheduler=self.resources[('deployment','scheduler')]['spec']['template']['spec']['containers'][0]['image']
        collector=self.resources[('job','collector')]['spec']['template']['spec']['containers'][0]
        self.assertNotEqual(server,scheduler);self.assertEqual(collector['image'],scheduler)
        self.assertIn('/openlegal-server-ingestion@sha256:',scheduler)
        self.native._emergency_current_configuration(self.op)
        collector['image']='ghcr.io/publicdata-stream/openlegal-server-ingestion@sha256:'+'7'*64
        with self.assertRaisesRegex(Refused,'profile_v2_mismatch'):
            self.native._emergency_current_configuration(self.op)
        collector['image']='ghcr.io/publicdata-stream/openlegal-server-ingestion:latest'
        with self.assertRaisesRegex(Refused,'project_template_image'):
            self.native._emergency_current_configuration(self.op)

    def test_selected_serving_template_must_match_the_actually_inspected_server(self):
        self.resources_fixture();self.bind_synthetic_profile()
        pod=self.resources[('deployment','serve')]['spec']['template']['spec']
        pod['containers'][0]['image']='ghcr.io/publicdata-stream/openlegal-server-ingestion@sha256:'+'8'*64
        wanted=next(row for row in self.op.inventory['linux']['workloads'] if row['name']=='serve')
        wanted['pod_spec_sha256']=digest(canonical(pod))
        with self.assertRaisesRegex(Refused,'serving_template_image'):
            self.native._emergency_current_configuration(self.op)

    def test_final_configuration_rejects_reconnect_after_fixed_end(self):
        self.resources_fixture(); self.bind_synthetic_profile(); self.observe()
        self.position['wal_receiver_count']=1
        with self.assertRaisesRegex(Refused,'receiver_changed_after_fixed_end'):
            self.native._emergency_current_configuration(self.op)


class FixedBoundaryTests(unittest.TestCase):
    def test_exact_backend_no_subclass_or_instance_callable(self):
        class Derived(linux.LinuxBackend):pass
        for backend in (Derived(),object()):
            with self.assertRaisesRegex(Refused,'fixed_linux_backend'):n.SurvivorNative(backend)
        backend=linux.LinuxBackend();adapter=n.SurvivorNative(backend)
        backend.callback=lambda:None
        with self.assertRaisesRegex(Refused,'fixed_linux_backend'):adapter._scope(None)

    def test_config_projection_rejects_unknown_top_level_fields(self):
        with self.assertRaisesRegex(Refused,'config_projection'):
            n.configuration_hash_v1({'unknown':'not-a-profile'})

    @unittest.skipUnless(os.geteuid()==0,'actual private command-receipt fixture requires root')
    def test_real_linux_runner_and_actual_waited_child_feed_the_sql_receipt_reader(self):
        fixture=authority_fixture.RecoveryTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        op=fixture.op;backend=linux.LinuxBackend();adapter=n.SurvivorNative(backend);self.addCleanup(adapter.close)
        pg=op.inventory['linux']['postgres'];pg.update(uid=70,database='openlegal',admin_user='survivor_reader')
        executable=Path(sys.executable).resolve()
        op.inventory['linux']['binaries']={'docker':{'path':str(executable),'sha256':digest(executable.read_bytes())}}
        identity={'database':'openlegal','session_user':'survivor_reader','current_user':'survivor_reader',
            'role':{'login':True,'superuser':False,'create_role':False,'create_db':False,
                    'replication':False,'bypass_rls':False,'inherit':False},
            'broad_roles':False,'owner_membership':False,'function_owners_qualified':True}
        raw=canonical({'identity':identity,'result':fixture.position})
        actual_execute=command_process.execute;children=[];exit_code=0
        def harmless(command,**kw):
            self.assertEqual(command[1:6],['--config',str(backend._docker_config[0]),'--host',
                                         'unix:///var/run/docker.sock','exec'])
            self.assertIn('/usr/bin/env',command);self.assertIn('PGSERVICE',command)
            # Only the innermost command is replaced with fixed harmless Python.
            # Actual held executable, OS wait and process ownership are retained.
            result=actual_execute([str(executable),'-c',
                "import os;os.write(1,bytes.fromhex("+repr(raw.hex())+"));os._exit("+str(exit_code)+")"],**kw)
            children.append(result);return result
        with patch.object(n.SurvivorNative,'_native',return_value={'fixed':'synthetic-qualified-native'}),\
             patch.object(command_process,'execute',harmless):
            ref=adapter._sql(op,fixture.subject,'position',time.monotonic()+5)
            exit_code=7
            with self.assertRaisesRegex(Refused,'command_failed_or_unclosed'):
                adapter._sql(op,fixture.subject,'position',time.monotonic()+5)
        if backend._docker_config:self.addCleanup(backend._docker_config[0].rmdir)
        self.assertEqual(e.read_evidence(op,ref,'position')['result'],fixture.position)
        self.assertEqual(len(children),2);self.assertEqual(children[0].actual_exit_code,0)
        self.assertEqual(children[1].actual_exit_code,7);self.assertTrue(children[1].owned_processes_closed)
        self.assertTrue(children[0].owned_processes_closed)
        receipt=decode(op.journal.values['verify_latest-command-0001.json'])
        self.assertEqual(receipt['stdout_hex'],raw.hex());self.assertEqual(receipt['actual_exit_code'],0)
        self.assertTrue(receipt['owned_processes_closed'])

    @unittest.skipUnless(os.geteuid()==0,'actual private bulk command-receipt fixture requires root')
    def test_real_linux_runner_bulk_snapshot_parses_both_large_raw_and_hex_receipt(self):
        fixture=authority_fixture.RecoveryTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        op=fixture.op;backend=linux.LinuxBackend();adapter=n.SurvivorNative(backend);self.addCleanup(adapter.close)
        pg=op.inventory['linux']['postgres'];pg.update(uid=70,database='openlegal',admin_user='survivor_reader')
        executable=Path(sys.executable).resolve()
        op.inventory['linux']['binaries']={'docker':{'path':str(executable),'sha256':digest(executable.read_bytes())}}
        identity={'database':'openlegal','session_user':'survivor_reader','current_user':'survivor_reader',
            'role':{'login':True,'superuser':False,'create_role':False,'create_db':False,
                    'replication':False,'bypass_rls':False,'inherit':False},
            'broad_roles':False,'owner_membership':False,'function_owners_qualified':True}
        snapshot=copy.deepcopy(fixture.snapshot);snapshot['retained_references']=[]
        for number in range(50103):
            sha=format(number,'064x')
            snapshot['retained_references'].append({'role':'corpus','sha256':sha,'size':number,
                'storage_key':sha[:2]+'/'+sha+'-00000000-0000-0000-0000-000000000001'})
        raw=canonical({'identity':identity,'result':snapshot})
        self.assertGreater(len(raw),4*1024**2);self.assertLess(len(raw),linux.MAX_OUTPUT)
        source=fixture.base/'synthetic-bulk-stdout.json';source.write_bytes(raw);source.chmod(0o600)
        actual_execute=command_process.execute;children=[]
        def harmless(command,**kw):
            self.assertIn('/usr/bin/env',command)
            self.assertIn(e.SNAPSHOT_SQL[7:].encode(),kw['stdin'])
            self.assertEqual(kw['max_output'],16*1024**2)
            result=actual_execute([str(executable),'-c',
                'import pathlib,sys;sys.stdout.buffer.write(pathlib.Path('+repr(str(source))+').read_bytes())'],**kw)
            children.append(result);return result
        try:
            with patch.object(n.SurvivorNative,'_native',return_value={'fixed':'synthetic-qualified-native'}),\
                 patch.object(command_process,'execute',harmless):
                ref=adapter._sql(op,fixture.subject,'snapshot',time.monotonic()+40)
        finally:
            if backend._docker_config:self.addCleanup(backend._docker_config[0].rmdir)
        receipt_raw=op.journal.values['verify_latest-command-0001.json']
        self.assertGreater(len(receipt_raw),8*1024**2)
        for value in (raw,receipt_raw):
            with self.assertRaisesRegex(Refused,'json_bound'):decode(value)
        receipt=e.manual._metadata_decode(receipt_raw)
        self.assertEqual(receipt['stdout_sha256'],digest(raw));self.assertEqual(receipt['stdout_hex'],raw.hex())
        self.assertEqual(receipt['actual_exit_code'],0);self.assertTrue(receipt['owned_processes_closed'])
        self.assertEqual(len(children),1);self.assertEqual(children[0].actual_exit_code,0)
        self.assertTrue(children[0].owned_processes_closed)
        result=e.read_evidence(op,ref,'snapshot')['result']
        self.assertEqual(len(result['retained_references']),50103)
        self.assertEqual(result,snapshot)


if __name__ == '__main__':unittest.main()
