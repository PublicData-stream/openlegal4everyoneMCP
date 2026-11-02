"""Shared projection semantics; no Docker, SQL, certificate or provider traffic."""
import copy
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import epoch_observation as epoch
import emergency_native as emergency
import initial_driver
import linux_backend
from interface import Refused
from inventory import canonical, digest


def projection():
    root = {'path':'/qualified/standby','dev':1,'ino':2,'uid':70,'gid':70,
            'mode':0o700,'kernel_filesystem_id':3}
    blob = dict(root,uid=10004,gid=10004,container_path='/blobs',
                filesystem_id='qualified-ssd',marker_sha256='a'*64)
    return {'schema_version':2,'kind':'openlegal_strict_config','topology':epoch.TOPOLOGY,
        'namespace':'openlegal','runtime':{
            'reference':'ghcr.io/publicdata-stream/openlegal-server@sha256:'+'b'*64,
            'docker_image_id':'sha256:'+'c'*64,'revision':'d'*40,'os':'linux','architecture':'amd64'},
        'deployments':[],'jobs':[],'configmaps':[],'persistent_volumes':[],
        'persistent_volume_claims':[],
        'service':{'kind':'service','name':'openlegal','namespace':'openlegal',
            'uid':'00000000-0000-4000-8000-000000000001','spec_sha256':'e'*64,
            'selector_sha256':'f'*64},
        'survivor':{'container_id':'1'*64,'image_id':'sha256:'+'2'*64,
            'data_directory':'/pg/data','data_root':root,'wal_root':dict(root,path='/qualified/standby/pg_wal',ino=3)},
        'blob_survivors':{'cache_replica':dict(blob,path='/qualified/cache',container_path='/cache'),
                          'corpus_primary':dict(blob,path='/qualified/corpus',container_path='/corpus')}}


class ProjectionTests(unittest.TestCase):
    def test_healthy_and_emergency_share_exact_hash_without_certificate_or_fs_reads(self):
        value=projection()
        with patch.object(os,'open',side_effect=AssertionError('pure hash opened filesystem')):
            self.assertIs(epoch.configuration_hash_v1,emergency.configuration_hash_v1)
            self.assertEqual(epoch.configuration_hash_v1(value),digest(epoch.DOMAIN+canonical(value)))
        self.assertEqual(epoch.SSD_SURVIVORS,('cache_replica','corpus_primary'))
        self.assertEqual(epoch.HDD_LOST,('cache_primary','corpus_replica'))

    def test_old_suffix_assumption_and_role_alias_are_rejected(self):
        for role in ('corpus_replica','cache_primary'):
            value=projection();value['blob_survivors'][role]=value['blob_survivors'].pop('corpus_primary')
            with self.subTest(role=role),self.assertRaisesRegex(Refused,'blob_roles'):
                epoch.configuration_hash_v1(value)
        value=projection();value['blob_replicas']=value.pop('blob_survivors')
        with self.assertRaisesRegex(Refused,'config_projection'):epoch.configuration_hash_v1(value)

    def test_topology_version_and_closed_root_changes_cannot_match(self):
        for key,value in (('topology','all_replicas_on_ssd'),('schema_version',1),('schema_version',True)):
            data=projection();data[key]=value
            with self.subTest(key=key,value=value),self.assertRaises(Refused):epoch.configuration_hash_v1(data)
        data=projection();data['blob_survivors']['corpus_primary']['unobserved']='invented'
        with self.assertRaises(Refused):epoch.configuration_hash_v1(data)

    def test_mutated_marker_runtime_or_service_changes_configuration_hash(self):
        before=projection();expected=epoch.configuration_hash_v1(before)
        for key in ('marker','runtime','service'):
            value=copy.deepcopy(before)
            if key=='marker':value['blob_survivors']['corpus_primary']['marker_sha256']='3'*64
            if key=='runtime':value['runtime']['revision']='4'*40
            if key=='service':value['service']['selector_sha256']='5'*64
            with self.subTest(key=key):self.assertNotEqual(epoch.configuration_hash_v1(value),expected)

    def test_common_native_ports_accept_only_exact_source_backends(self):
        class Unknown(linux_backend.LinuxBackend):pass
        for backend in (Unknown(),object()):
            with self.subTest(backend=type(backend).__name__),self.assertRaisesRegex(Refused,'fixed_backend'):
                epoch.NativeObservation(backend)
        backend=linux_backend.LinuxBackend();backend.override=lambda:None
        with self.assertRaisesRegex(Refused,'fixed_backend'):epoch.NativeObservation(backend)

    def test_driver_original_deadline_caps_native_reader_without_new_window(self):
        backend=initial_driver.Backend();backend._driver_deadline=time.monotonic()+1
        common=epoch.NativeObservation(backend)
        self.assertEqual(common._deadline(time.monotonic()+100),backend._driver_deadline)
        for invalid in (float('nan'),float('inf'),float('-inf')):
            backend._driver_deadline=invalid
            with self.subTest(original=invalid),self.assertRaisesRegex(Refused,'original_driver_deadline'):
                common._deadline(time.monotonic()+100)
        backend._driver_deadline=time.monotonic()-1
        with self.assertRaisesRegex(Refused,'original_deadline'):common._deadline(time.monotonic()+100)

    def test_real_tmp_native_descriptor_reads_need_no_emergency_authority(self):
        with tempfile.TemporaryDirectory(prefix='epoch-native-',dir='/tmp') as name:
            fd=epoch._directory(name)
            try:
                row=epoch._descriptor(name,fd)
                self.assertEqual((row['dev'],row['ino']),(os.fstat(fd).st_dev,os.fstat(fd).st_ino))
                self.assertEqual(epoch._fd_identity(fd)[-1],epoch._fd_mount_id(fd))
            finally:os.close(fd)


if __name__=='__main__':unittest.main()
