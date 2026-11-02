"""Standby-only native SurvivorReadPort adapter. No recovery dispatch or CLI.

The future pre-ACK producer must use configuration_hash_v1 on the same closed
projection before public admission. This module does not install that certificate.
Only the selected surviving directories are opened; old-primary pins are inputs
and fencing subjects, never healthy native observations.

The current source-graph registry/failed-primary driver/pre-ACK producer do not
register this adapter. Installing those integration pieces requires separate
review. Inline PGDATA/pg_wal is the only accepted native layout in this profile.
"""
from __future__ import annotations

import math
import os
from pathlib import Path
import platform
import re
import stat
import time

import emergency_backend as emergency
import linux_backend as linux
from interface import need
from inventory import canonical, decode, digest, pin, private_bytes, valid_uuid

SNAPSHOT_SECONDS = 40
CONFIGURATION_SECONDS = 120
from epoch_observation import INSPECT, IMAGE, UNSET, IDENTITY

# One implementation is shared with the healthy pre-ACK producer. Underscore
# aliases preserve internal callers; hash validation accepts schema2 only.
import epoch_observation as observation
from epoch_observation import (DOMAIN, PROFILE_FIELDS, configuration_hash_v1,
    _plain, _shape, _check, _descriptor, _directory, _fd_mount_id, _fd_identity,
    _kernel_mountinfo, _mounts, _mount_pair, _inline_layout)


class SurvivorNative(observation.NativeObservation):
    """Wrap the exact existing backend; no inventory callbacks or command text."""
    def __init__(self,backend):
        need(type(backend) is linux.LinuxBackend and
             not any(callable(v) for v in vars(backend).values()), 'emergency_fixed_linux_backend')
        super().__init__(backend);self.ordinals={};self.fixed_receive={}


    def _scope(self,op):
        need(type(self.backend) is linux.LinuxBackend and
             not any(callable(v) for v in vars(self.backend).values()), 'emergency_fixed_linux_backend')
        need(op.phase == 'verify_latest', 'emergency_native_phase');op.seal()
        for name in ('emergency_native.py','emergency_backend.py','epoch_observation.py','linux_backend.py','interface.py','inventory.py'):
            need(name in op.inventory['source_graph'], 'emergency_native_source_graph')
            private_bytes(Path(__file__).resolve().parent/name,op.inventory['source_graph'][name])
        return _plain(emergency.validate_authority(op)['certificate']['subject'])

    def _save(self,op,kind,payload):
        if kind == 'snapshot':return emergency.save_evidence(op,kind,payload)
        key=(op.operation_id,op.phase,kind);n=self.ordinals.get(key,0)+1;self.ordinals[key]=n
        return emergency.save_evidence(op,kind,payload,ordinal=n)




    def _sql(self,op,subject,kind,deadline):
        value=self._query_metadata(op,subject,kind,deadline)
        reference=self._save(op,kind,{k:value[k] for k in ('container_id','sql','result')})
        self._deadline(deadline);return reference

    def _closed(self,op,subject,deadline):
        while True:
            ref=self._sql(op,subject,'sql-closure',deadline)
            value=emergency.read_evidence(op,ref,'sql-closure')['result']
            if value == {'sessions':[]}:return ref
            _check(deadline);time.sleep(.05)

    def _fixed(self,op,subject,position,*,select=False):
        emergency._position(position,subject)
        key=(op.operation_id,op.phase)
        if select and position['wal_receiver_count']==0 and key not in self.fixed_receive:
            self.fixed_receive[key]=position['receive_lsn']
        if key in self.fixed_receive:
            need(position['wal_receiver_count']==0 and position['receive_lsn']==self.fixed_receive[key],
                 'emergency_receiver_changed_after_fixed_end')

    def _emergency_survivor_observe(self,op,*,container_id,deadline):
        subject=self._scope(op);need(container_id==subject['standby']['container_id'],'emergency_standby_only')
        _check(deadline);ref=self._sql(op,subject,'position',deadline)
        self._fixed(op,subject,emergency.read_evidence(op,ref,'position')['result'],select=True)
        self._closed(op,subject,deadline);return ref

    def _emergency_survivor_snapshot(self,op,*,container_id):
        end=time.monotonic()+SNAPSHOT_SECONDS
        subject=self._scope(op);need(container_id==subject['standby']['container_id'],'emergency_standby_only')
        _check(end)
        ref=self._sql(op,subject,'snapshot',end)
        result=emergency.read_evidence(op,ref,'snapshot')['result']
        for key in ('position_before','position_after'):self._fixed(op,subject,result[key])
        self._closed(op,subject,end);return ref

    def _emergency_survivor_sql_closed(self,op,*,container_id):
        end=time.monotonic()+15
        subject=self._scope(op);need(container_id==subject['standby']['container_id'],'emergency_standby_only')
        return self._closed(op,subject,end)


    def _emergency_current_configuration(self,op):
        end=time.monotonic()+CONFIGURATION_SECONDS
        subject=self._scope(op);_check(end);config=op.inventory['linux']
        position_ref=self._sql(op,subject,'position',end)
        position=emergency.read_evidence(op,position_ref,'position')['result'];self._fixed(op,subject,position)
        current=self._configuration_projection(op,subject,end)
        projection=current['projection'];native=current['native'];sha=current['configuration_sha256']
        need(sha==subject['runtime']['configuration_sha256'],'emergency_config_profile_v2_mismatch')
        need(self._native(op,subject,end)==native,'emergency_configuration_native_changed')
        self._closed(op,subject,end);op.seal();_check(end)
        self.backend._artifact(op,'emergency-native-config-'+str(self.ordinals.get((op.operation_id,op.phase,'configuration'),0)+1),
            {'profile':'openlegal-strict-config-schema2','projection':projection,'configuration_sha256':sha,
             'fresh_blob_roles':list(observation.SSD_SURVIVORS),'unavailable_originals_not_opened':True})
        reference=self._save(op,'configuration',{'configuration_sha256':sha,'runtime_image_id':projection['runtime']['docker_image_id'],
            'runtime_revision':projection['runtime']['revision'],'primary_container_id':config['postgres']['primary']['container_id'],
            'standby_container_id':native['container_id'],'blob_binding_sha256':position['control']['blob_binding_sha256']})
        _check(end);return reference
