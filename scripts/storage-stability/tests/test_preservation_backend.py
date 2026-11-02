"""Pure subset rules and explicitly synthetic readonly SQL evidence only."""
import copy
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import preservation_backend as p
from interface import Refused


def row(key='a',sha='a'*64,**monotonic):
    if len(key)!=64:key=key.encode().hex().ljust(64,'0')
    return {'key':[key],'sha256':sha,'monotonic':monotonic or None}


def page(rows,kind='corpus_capture',more=False):
    return {'schema_version':1,'kind':'openlegal_preservation_page','relation':kind,
            'rows':copy.deepcopy(rows),'has_more':more,
            'next_key':copy.deepcopy(rows[-1]['key']) if more else None}


class PermanentSubset(unittest.TestCase):
    def test_append_preserves_every_old_row(self):
        old = [page([row('a'),row('c')])]
        new = [page([row('a'),row('b'),row('c')])]
        result = p.compare_subset('corpus_capture',old,new)
        self.assertEqual((result['baseline_rows'],result['final_rows']),(2,3))

    def test_equal_counts_cannot_hide_old_deletion(self):
        with self.assertRaisesRegex(Refused,'old_row_missing'):
            p.compare_subset('corpus_capture',[page([row('a')])],[page([row('b')])])

    def test_row_hash_mutation_is_never_a_counter_exception(self):
        with self.assertRaisesRegex(Refused,'old_row_changed'):
            p.compare_subset('corpus_capture',[page([row()])],[page([row(sha='b'*64)])])

    def test_only_explicit_monotonic_fields_can_advance(self):
        kind='corpus_source_observation'
        old = [page([row(validated_at='10')],kind)]
        p.compare_subset(kind,old,[page([row(validated_at='11')],kind)])
        with self.assertRaisesRegex(Refused,'counter_regressed'):
            p.compare_subset(kind,old,[page([row(validated_at='9')],kind)])
        with self.assertRaisesRegex(Refused,'monotonic_schema'):
            p.validate_page(kind,page([row(validated_at=None)],kind))

    def test_monotonic_schema_cannot_silently_change(self):
        with self.assertRaisesRegex(Refused,'monotonic_schema'):
            p.validate_page('corpus_source_observation',page([row()],'corpus_source_observation'))
        immutable = dict(row(),monotonic=None)
        p.compare_subset('corpus_capture',[page([immutable])],[page([immutable])])
        with self.assertRaisesRegex(Refused,'monotonic_schema'):
            p.validate_page('corpus_capture',page([dict(row(),monotonic={})]))

    def test_initial_null_can_be_filled_without_float_rounding(self):
        kind='corpus_object_identity'
        old=row(version='0',catalog_version='0',next_capture='1',validated_at=None)
        new=copy.deepcopy(old);new['monotonic']['validated_at']='18446744073709551615'
        result = p.compare_subset(kind,[page([old],kind)],[page([new],kind)])
        self.assertTrue(result['monotonic_fields_preserved'])
        for value in ('18446744073709551616','01','-1','1e9',True,1.0):
            with self.subTest(value=value),self.assertRaises(Refused):p.integer(value)

    def test_full_page_chain_rejects_truncation_and_duplicate_cursor(self):
        rows = [row(str(i).zfill(3)) for i in range(128)]
        first = page(rows,more=True)
        with self.assertRaisesRegex(Refused,'page_chain'):
            p.compare_subset('corpus_capture',[first],[first])
        final = page([row('128')])
        p.compare_subset('corpus_capture',[first,final],[first,final])
        final['rows'][0]['key'] = row('127')['key']
        with self.assertRaisesRegex(Refused,'order_or_duplicate'):
            p.compare_subset('corpus_capture',[first,final],[first,final])

    def test_native_numeric_blob_ordinal_order_is_not_lexicographic(self):
        rows = [dict(row(),key=[row('capture')['key'][0],str(i)]) for i in (2,10)]
        p.validate_page('corpus_capture_blob',page(rows,'corpus_capture_blob'))
        rows.reverse()
        with self.assertRaisesRegex(Refused,'order_or_duplicate'):
            p.validate_page('corpus_capture_blob',page(rows,'corpus_capture_blob'))

    def test_wrong_relation_extra_payload_and_bad_final_cursor_refuse(self):
        cases = [page([row()],'cache_snapshot'),dict(page([row()]),body='forbidden'),
                 dict(page([row()]),next_key=['a'])]
        for value in cases:
            with self.subTest(value=value),self.assertRaises(Refused):
                p.validate_page('corpus_capture',value)

    def test_exact_relation_key_and_counter_schemas_refuse_malformed_metadata(self):
        cases = [dict(row(),key=['a'*64,'b'*64]),dict(row(),key=['https://private.invalid/?key=secret']),
                 row(validated_at='1')]
        for value in cases:
            with self.subTest(value=value),self.assertRaises(Refused):
                p.validate_page('corpus_capture',page([value]))
        with self.assertRaisesRegex(Refused,'monotonic_schema'):
            p.validate_page('corpus_source_observation',page([row()],'corpus_source_observation'))
        invalid = row(version='0',catalog_version='0',next_capture='1',validated_at=None)
        invalid['monotonic']['version'] = str(2**63)
        with self.assertRaisesRegex(Refused,'counter_domain'):
            p.validate_page('corpus_object_identity',page([invalid],'corpus_object_identity'))

    def test_ready_generation_identity_is_preserved_even_if_unreferenced(self):
        old = [{'storage_key':'aa/'+'a'*64+'-11111111-1111-4111-8111-111111111111','sha256':'a'*64,'size':4}]
        new = old+[{'storage_key':'aa/'+'a'*64+'-22222222-2222-4222-8222-222222222222','sha256':'a'*64,'size':4}]
        self.assertTrue(p.compare_generations(old,new)['all_baseline_generations_preserved'])
        with self.assertRaisesRegex(Refused,'old_generation_missing'):
            p.compare_generations(old,new[1:])
        mutated = copy.deepcopy(old);mutated[0]['size'] = 5
        with self.assertRaisesRegex(Refused,'old_generation_missing'):
            p.compare_generations(old,mutated)
        for key in ('','/tmp/escape','https://private.invalid/blob',
                    'ab/'+old[0]['storage_key'][3:],old[0]['storage_key'].replace('a'*64,'b'*64)):
            with self.subTest(key=key),self.assertRaisesRegex(Refused,'generation_key'):
                p.compare_generations([dict(old[0],storage_key=key)],old)


class Journal:
    def __init__(self):self.values = {};self.fail = False
    def save(self,name,value):
        if self.fail:raise OSError('synthetic_second_media_failure')
        self.values[name] = copy.deepcopy(value)
        return p.digest(p.canonical(value))


class FakeBackend:
    def __init__(self,value):self._artifacts = {'prior':'b'*64};self.value = value;self.reads = []
    def _artifact(self,op,name,value):
        filename = op.phase+'-'+name+'.json'
        self._artifacts[filename] = op.journal.save(filename,value)
    def _sql(self,op,sql,**kwargs):
        self.reads.append(sql)
        from linux_backend import SQL_SESSION_PREFIX
        raw=p.canonical(self.value)
        self._artifact(op,'command',{'binary':'docker','actual_exit_code':0,'stdout_sha256':p.digest(raw),
            'stdout_hex':raw.hex(),'stdout_bytes':len(raw),
            'stdin_sha256':p.digest((SQL_SESSION_PREFIX+sql+';\n').encode()),
            'owned_processes_closed':True,'started':True,'expired':False,'output_limit_exceeded':False,
            'unexpected_descendants':False,'interrupted':False})
        return copy.deepcopy(self.value)


class ReadonlyEvidence(unittest.TestCase):
    def setUp(self):
        self.journal = Journal()
        self.op = SimpleNamespace(operation_id=str(uuid.uuid4()),action='canary',phase='preservation',
            inventory={'managed_epoch':str(uuid.uuid4()),'linux':{'postgres':{'primary':{'container_id':'d'*64}}}},
            inventory_sha256='a'*64,source_graph_sha256='b'*64,
            authority_sha256='c'*64,authority={},previous={},seal=lambda:None,journal=self.journal)

    def test_one_actual_command_bundle_ack_precedes_metadata_acceptance(self):
        backend = FakeBackend(page([row()]))
        pages = p.scan_relation(backend,self.op,'corpus_capture',deadline=time.monotonic()+5)
        self.assertEqual(len(pages),1);self.assertEqual(len(self.journal.values),1)
        bundle = next(iter(self.journal.values.values()))
        self.assertEqual(bundle['result'],pages[0])
        self.assertTrue(bundle['actual_command_evidence'])
        self.assertIn('prior',backend._artifacts)
        self.assertNotIn('preservation-command.json',backend._artifacts)

    def test_second_media_failure_never_returns_read_success(self):
        backend = FakeBackend(page([]));self.journal.fail = True
        with self.assertRaisesRegex(OSError,'second_media'):
            p.scan_relation(backend,self.op,'corpus_capture',deadline=time.monotonic()+5)

    def test_unknown_relation_and_expired_scan_never_dispatch(self):
        backend = FakeBackend(page([]))
        with self.assertRaisesRegex(Refused,'relation_whitelist'):
            p.scan_relation(backend,self.op,"corpus_capture');DROP TABLE x;--",deadline=time.monotonic()+5)
        with self.assertRaisesRegex(Refused,'scan_deadline'):
            p.scan_relation(backend,self.op,'corpus_capture',deadline=time.monotonic()-1)
        self.assertEqual(backend.reads,[])

    def test_no_command_evidence_and_unclosed_command_never_return_page(self):
        backend = FakeBackend(page([]))
        backend._sql = lambda op,query,**kwargs:page([])
        with self.assertRaisesRegex(Refused,'actual_command_evidence_required'):
            p.scan_relation(backend,self.op,'corpus_capture',deadline=time.monotonic()+5)
        self.assertTrue(next(iter(self.journal.values.values()))['failed'])
        self.journal.values.clear()
        backend = FakeBackend(page([]));original = backend._sql
        def unclosed(op,sql,**kwargs):
            value=original(op,sql,**kwargs)
            record=next(iter(op.journal.values.values()))
            record['value']['owned_processes_closed']=False
            record['sha256']=p.digest(p.canonical(record['value']))
            return value
        backend._sql=unclosed
        with self.assertRaisesRegex(Refused,'command_not_closed'):
            p.scan_relation(backend,self.op,'corpus_capture',deadline=time.monotonic()+5)

    def test_wrong_command_input_or_output_never_substitutes_actual_sql(self):
        for field,bad in [('stdin_sha256','0'*64),('stdout_sha256','0'*64)]:
            self.journal.values.clear()
            backend=FakeBackend(page([]));original=backend._sql
            def altered(op,sql,**kwargs):
                value=original(op,sql,**kwargs)
                record=next(iter(op.journal.values.values()))
                record['value'][field]=bad
                record['sha256']=p.digest(p.canonical(record['value']))
                return value
            backend._sql=altered
            with self.subTest(field=field),self.assertRaises(Refused):
                p.scan_relation(backend,self.op,'corpus_capture',deadline=time.monotonic()+5)
