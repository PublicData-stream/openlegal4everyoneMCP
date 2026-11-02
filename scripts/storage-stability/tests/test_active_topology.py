"""Draft active-topology protocol tests. Disposable /tmp metadata only.
No actual dataset, PostgreSQL, snapshot, Docker or Kubernetes action is run.
"""
import copy
import types
import unittest
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import interface
import inventory
import checkpoint
import test_storage_stability as base


class ActiveTopologyTests(base.Fixture):
    prepared=base.WorkflowTests.prepared
    roots=base.JournalTests.roots
    fake_independent=base.JournalTests.fake_independent

    def active(self):
        value=self.prepared()[0]
        extra={'name':'pool/new-hdd-postgres','guid':'900','pool':'pool','mountpoint':str(self.root/'new-hdd-postgres')}
        value['active_datasets']=[extra];value['coverage']['postgres'].append(extra['name'])
        value['postgres']={'synthetic_fixture':True,'primary':{'tree':'postgres'},'standby':{'dataset':extra['name']}}
        return value

    def receipt(self,value):
        operation='11111111-1111-4111-8111-111111111111';outputs={'synthetic.json':'a'*64}
        op=types.SimpleNamespace(operation_id=operation,action='checkpoint',phase='snapshot_same_pool',inventory=value,
            inventory_sha256='b'*64,source_graph_sha256='c'*64,authority_sha256='d'*64,
            journal=types.SimpleNamespace(output_hashes=lambda:outputs),previous={})
        facts={name:True for name in interface.REQUIRED_TRUE[op.phase]}
        facts.update(index_sha256='e'*64,latest_wal_lsn='1/ABC',snapshots=[
            {'dataset':d['name'],'guid':d['guid'],'snapshot':d['name']+'@openlegal-checkpoint-'+operation.replace('-','')[:8],
             'snapshot_guid':str(1000+i)}for i,d in enumerate(inventory.all_datasets(value))])
        receipt={'schema_version':1,'kind':interface.RECEIPT_KIND,'operation_id':operation,'action':op.action,'phase':op.phase,
            'inventory_sha256':op.inventory_sha256,'source_graph_sha256':op.source_graph_sha256,'authority_sha256':op.authority_sha256,
            'status':'passed','actual_exit_code':0,'owned_processes_closed':True,'facts':facts,'artifacts':outputs}
        return op,receipt

    def test_original_only_inventory_still_snapshots_exact_seven(self):
        value=self.prepared()[0];self.assertEqual(value['active_datasets'],[])
        inventory.validate_inventory(value);op,receipt=self.receipt(value)
        checkpoint.validate_receipt({'interface':interface,'inventory':inventory},op,receipt)
        self.assertEqual(len(receipt['facts']['snapshots']),7)

    def test_post_promotion_ssd_primary_new_hdd_peer_keeps_original_seven(self):
        value=self.active();before=inventory.canonical(value['original_datasets'])
        inventory.validate_inventory(value);rows=inventory.all_datasets(value)
        self.assertEqual(len(rows),8);self.assertEqual(inventory.canonical(value['original_datasets']),before)
        rows[0]['guid']='changed';rows.pop()
        self.assertEqual(inventory.canonical(value['original_datasets']),before)
        self.assertEqual(value['active_datasets'][0]['guid'],'900')
        op,receipt=self.receipt(value);checkpoint.validate_receipt({'interface':interface,'inventory':inventory},op,receipt)
        self.assertEqual(len(receipt['facts']['snapshots']),8)

    def test_active_pool_name_guid_mount_and_source_overlap_refuse(self):
        value=self.active()
        for kind in ('pool','name','guid','mount','nested_dataset','nested_path','ssd_path','backup','uncovered','bound'):
            bad=copy.deepcopy(value);extra=bad['active_datasets'][0]
            if kind=='pool':extra.update(name='other/new-hdd-postgres',pool='other')
            elif kind=='name':extra['name']=bad['original_datasets'][0]['name']
            elif kind=='guid':extra['guid']=bad['original_datasets'][0]['guid']
            elif kind=='mount':extra['mountpoint']=bad['original_datasets'][0]['mountpoint']
            elif kind=='nested_dataset':extra['name']=bad['original_datasets'][0]['name']+'/child'
            elif kind=='nested_path':extra['mountpoint']=bad['original_datasets'][0]['mountpoint']+'/child'
            elif kind=='ssd_path':extra['mountpoint']=bad['ssd_trees'][0]['path']
            elif kind=='backup':extra['mountpoint']=bad['backup_root']['path']+'/source'
            elif kind=='uncovered':bad['coverage']['postgres'].remove(extra['name'])
            else:bad['active_datasets']=[dict(extra,name='pool/active'+str(i),guid=str(900+i),mountpoint=str(self.root/('active'+str(i))))for i in range(17)]
            with self.subTest(kind=kind),self.assertRaises(interface.Refused):inventory.validate_inventory(bad)

    def test_original_seven_and_generic_two_media_pg_coverage_cannot_be_downgraded(self):
        value=self.active()
        for kind in ('original','no_hdd','no_ssd','legacy_role'):
            bad=copy.deepcopy(value)
            if kind=='original':bad['original_datasets'].pop()
            elif kind=='no_hdd':bad['coverage']['postgres']=['postgres']
            elif kind=='no_ssd':bad['coverage']['postgres']=[bad['active_datasets'][0]['name']]
            else:bad['coverage']['standby_pg']=bad['coverage'].pop('postgres')
            with self.subTest(kind=kind),self.assertRaises(interface.Refused):inventory.validate_inventory(bad)

    def test_snapshot_exact_union_source_guids_and_single_group_mandatory(self):
        value=self.active();op,receipt=self.receipt(value)
        for kind in ('missing_active','missing_original','wrong_guid','duplicate','wrong_group','duplicate_snapshot_guid','missing_active_fact'):
            bad=copy.deepcopy(receipt);rows=bad['facts']['snapshots']
            if kind=='missing_active':rows.pop()
            elif kind=='missing_original':rows.pop(0)
            elif kind=='wrong_guid':rows[-1]['guid']='901'
            elif kind=='duplicate':rows[-1]=copy.deepcopy(rows[0])
            elif kind=='wrong_group':rows[-1]['snapshot']+='-other'
            elif kind=='duplicate_snapshot_guid':rows[-1]['snapshot_guid']=rows[0]['snapshot_guid']
            else:bad['facts'].pop('active_datasets_exact')
            with self.subTest(kind=kind),self.assertRaises(interface.Refused):checkpoint.validate_receipt({'interface':interface,'inventory':inventory},op,bad)


if __name__=='__main__':unittest.main()
