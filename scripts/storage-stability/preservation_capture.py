"""Fixed quiet preservation captures; no publication, activation or OS-exit claim.

Only the sealed native backend is supported. The caller holds SharedLock and
retains the real writer cgroup descriptors. Baseline and final are separate,
append-only phases; failed baselines are never adopted or overwritten here.
"""
from __future__ import annotations

import copy
import math
import os
from pathlib import Path
import re
import time
from types import SimpleNamespace

import bootstrap_backend as bootstrap
import preservation_backend as p
import provider_backend as provider
from interface import need
from inventory import canonical, decode, digest, pin, private_bytes, valid_uuid
from journal import directory, readback, NAME

MAX_INDEX = 4*1024**2
CHUNK = 128
ROLES = ('cache_primary','cache_replica','corpus_primary','corpus_replica')
INDEX_NAME = 'preservation-index.json'
REF_FIELDS = {'schema_version','kind','operation_id','phase','name','sha256','journal_phase_roots'}
BUNDLE_FIELDS = {'schema_version','kind','operation_id','phase','managed_epoch','inventory_sha256',
                 'source_graph_sha256','authority_sha256','target_container_id','query_sha256',
                 'result','failed','actual_command_evidence'}


def _deadline(deadline):
    need(type(deadline) in (int,float) and math.isfinite(deadline)
         and time.monotonic() < deadline,'preservation_capture_deadline')


def _scope(op, final):
    action,phase,permission = ('canary','preservation_final','canary_final_preservation_permitted') if final else (
        'strict_prepare','preservation_baseline','preservation_baseline_permitted')
    need(os.geteuid() == 0 and op.action == action and op.phase == phase and op.authority.get(permission) is True,
         'preservation_capture_authority')
    need(valid_uuid(op.operation_id) and valid_uuid(op.inventory['managed_epoch'])
         and all(pin(getattr(op,k)) for k in ('inventory_sha256','source_graph_sha256','authority_sha256'))
         and digest(canonical(op.inventory['source_graph'])) == op.source_graph_sha256,
         'preservation_capture_identity')
    op.seal()


def _identity(op):
    return {key:getattr(op,key) for key in ('operation_id','action','phase','inventory_sha256',
        'source_graph_sha256','authority_sha256')} | {
        'managed_epoch':op.inventory['managed_epoch'],
        'target_container_id':op.inventory['linux']['postgres']['primary']['container_id']}


def _phase_roots(op):
    roots = op.inventory['journal_roots']
    need(type(roots) is list and len(roots) == 2 and roots[0]['identity']['dev'] != roots[1]['identity']['dev'],
         'preservation_independent_journals')
    actual = op.journal.current()
    need(type(actual) is list and len(actual) == 2,'preservation_phase_journals')
    result = []
    for root,(path,expected) in zip(roots,actual):
        base = Path(root['path']);directory(base,root['identity'])
        operation = base/op.operation_id; op_identity = directory(operation)
        wanted = operation/op.phase
        need(Path(path) == wanted,'preservation_phase_journal_path')
        result.append({'path':str(wanted),'identity':directory(wanted,expected),
                       'operation_identity':op_identity})
    op.journal.check()
    return result


def _ref(name, sha, **fields):
    need(type(name) is str and NAME.fullmatch(name) and pin(sha),'preservation_artifact_reference')
    return {'name':name,'sha256':sha,**fields}


def _saved(backend,op,name,value):
    backend._artifact(op,name,value)
    filename = op.phase+'-'+name+'.json';raw = canonical(value);sha = digest(raw)
    need(op.journal.output_hashes().get(filename) == sha
         and op.journal.read_phase_bytes(op.phase,filename,sha) == raw,
         'preservation_capture_acknowledgement')
    return _ref(filename,sha)


def _query(backend,op,sql,name,deadline):
    result = p._query_bundle(backend,op,sql,name,deadline)
    filename = op.phase+'-'+name+'.json'
    hashes = op.journal.output_hashes();need(filename in hashes,'preservation_query_missing')
    ref = _ref(filename,hashes[filename])
    value = decode(op.journal.read_phase_bytes(op.phase,filename,ref['sha256']))
    _bundle(value,_identity(op),sql)
    need(value['result'] == result,'preservation_query_result_binding')
    return result,ref


def _witness(records,sql,result):
    from linux_backend import SQL_SESSION_PREFIX
    extra = "SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; "
    inputs = {digest((SQL_SESSION_PREFIX+suffix+sql+';\n').encode()) for suffix in ('',extra)}
    need(type(records) is dict and 1 <= len(records) <= 128,'preservation_command_records')
    witnessed = False
    for name,record in records.items():
        need(type(name) is str and NAME.fullmatch(name) and type(record) is dict
             and set(record) == {'sha256','value'} and pin(record['sha256'])
             and digest(canonical(record['value'])) == record['sha256'],'preservation_command_record_hash')
        value = record['value']
        if not (type(value) is dict and value.get('binary') == 'docker'
                and value.get('stdin_sha256') in inputs):continue
        need(type(value.get('actual_exit_code')) is int and value['actual_exit_code'] == 0
             and value.get('started') is True and value.get('owned_processes_closed') is True
             and all(value.get(k) is False for k in ('expired','output_limit_exceeded',
                 'unexpected_descendants','interrupted')),'preservation_command_not_closed')
        raw = value.get('stdout_hex')
        need(type(raw) is str and re.fullmatch('[0-9a-f]*',raw) is not None
             and len(raw)%2 == 0 and len(raw) <= p.MAX_BUNDLE_BYTES,'preservation_command_stdout')
        data = bytes.fromhex(raw)
        need(type(value.get('stdout_bytes')) is int and len(data) == value['stdout_bytes']
             and digest(data) == value.get('stdout_sha256') and decode(data) == result,
             'preservation_command_result_binding')
        witnessed = True
    need(witnessed,'preservation_actual_query_witness')


def _bundle(value,identity,sql):
    need(type(value) is dict and set(value) == BUNDLE_FIELDS and value['schema_version'] == 1
         and type(value['schema_version']) is int and value['kind'] == 'openlegal_preservation_query_bundle'
         and value['failed'] is False and value['query_sha256'] == digest(sql.encode())
         and all(value[k] == identity[k] for k in ('operation_id','phase','managed_epoch',
            'inventory_sha256','source_graph_sha256','authority_sha256','target_container_id')),
         'preservation_query_bundle_binding')
    _witness(value['actual_command_evidence'],sql,value['result'])
    return value['result']


def _closure(backend,op,stage,deadline):
    _deadline(deadline)
    buffer = p.CommandBuffer(); old = dict(backend._artifacts)
    query_op = SimpleNamespace(**{**vars(op),'journal':buffer})
    result = None; failed = True
    try:
        result = backend._closed_writers(query_op)
        _witness(buffer.values,'SELECT openlegal_checkpoint.client_sessions()',{'sessions':[]})
        need(type(result) is dict and set(result) == provider.CLOSURE_FIELDS
             and result['sql_sessions'] == [],'preservation_physical_closure')
        _deadline(deadline);failed = False
    finally:
        backend._artifacts.clear();backend._artifacts.update(old)
        ref = _saved(backend,op,'capture-'+stage+'-closure',{
            'schema_version':1,'kind':'openlegal_preservation_physical_closure',**_identity(op),
            'result':result,'failed':failed,'actual_command_evidence':buffer.values})
    return result,ref


def _position(value):
    need(type(value) is dict and set(value) == {'system_identifier','timeline','postmaster_started_at',
        'wal_lsn','index_ack','watermark','in_recovery'} and value['in_recovery'] is False
        and type(value['system_identifier']) is str and re.fullmatch('[1-9][0-9]{0,19}',value['system_identifier'])
        and type(value['timeline']) is int and 1 <= value['timeline'] <= 4294967295
        and type(value['postmaster_started_at']) is str and 1 <= len(value['postmaster_started_at']) <= 128
        and all(type(value[k]) is int and 0 <= value[k] <= p.I64 for k in ('watermark','index_ack')),
        'preservation_database_position')
    _lsn(value['wal_lsn'])
    return value


def _lsn(value):
    need(type(value) is str and re.fullmatch('[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}',value),
         'preservation_wal_lsn')
    a,b = value.split('/');return int(a,16)*2**32+int(b,16)


def _stage(backend,op,stage,deadline,final):
    closure,closure_ref = _closure(backend,op,stage,deadline)
    sql = "SELECT openlegal_checkpoint.observe('%s'::uuid)"%op.operation_id
    held,held_ref = _query(backend,op,sql,'capture-'+stage+'-provider',deadline)
    projection = provider._project(held)
    need(projection['clean'] and projection['recovery_hold']
         and held['state']['admissions'] == [] and held['admission_ownership']['slots'] == []
         and (not final or projection['owner_operation_id'] == op.operation_id),
         'preservation_held_clean_provider')
    position,position_ref = _query(backend,op,'SELECT openlegal_checkpoint.database_position()',
                                 'capture-'+stage+'-position',deadline)
    _position(position)
    barrier_deadline = min(deadline,time.monotonic()+2)
    first,first_ref = _query(backend,op,'SELECT openlegal_replication.capture_prefix()',
                            'capture-'+stage+'-replica-prefix',barrier_deadline)
    second,second_ref = _query(backend,op,'SELECT openlegal_replication.observe_flush()',
                              'capture-'+stage+'-replica-flush',barrier_deadline)
    _deadline(barrier_deadline);_replica(backend,op,first,second,position)
    return {'closure':closure_ref,'provider':held_ref,'position':position_ref,
            'replica_prefix':first_ref,'replica_flush':second_ref}, {
                'provider':projection,'position':position,'replica':second,'closure':closure}


def _replica(backend,op,first,second,position):
    backend._check_replica_observation(op,first);backend._check_replica_observation(op,second)
    static = set(first)-{'prefix_lsn','flush_lsn'}
    need(all(first[k] == second[k] for k in static)
         and _lsn(second['flush_lsn']) >= _lsn(first['prefix_lsn']) >= _lsn(position['wal_lsn'])
         and _lsn(second['prefix_lsn']) >= _lsn(first['prefix_lsn'])
         and first['primary_system_identifier'] == position['system_identifier']
         and first['timeline'] == position['timeline']
         and first['postmaster_started_at'] == position['postmaster_started_at'],
         'preservation_replica_position_binding')


def _stable(before,after):
    need(before['provider'] == after['provider'],'preservation_provider_changed_during_scan')
    a,b = before['position'],after['position']
    need(all(a[k] == b[k] for k in ('system_identifier','timeline','postmaster_started_at','watermark','index_ack'))
         and _lsn(b['wal_lsn']) >= _lsn(a['wal_lsn']),'preservation_database_changed_during_scan')
    static = set(before['replica'])-{'prefix_lsn','flush_lsn'}
    need(all(before['replica'][k] == after['replica'][k] for k in static),
         'preservation_replica_changed_during_scan')


class _BudgetBackend:
    """Fixed native forwarding only; no configurable command/callback input."""
    def __init__(self, backend):
        self.backend = backend;self._artifacts = backend._artifacts;self.pages = 0;self.rows = 0
    def _artifact(self,*args):return self.backend._artifact(*args)
    def _sql(self,op,sql,**kwargs):
        need(self.pages < p.MAX_PAGES,'preservation_global_page_bound')
        value = self.backend._sql(op,sql,**kwargs)
        kind = value.get('relation') if type(value) is dict else None
        p.validate_page(kind,value)
        need(self.rows+len(value['rows']) <= p.MAX_ROWS,'preservation_global_row_bound')
        self.pages += 1;self.rows += len(value['rows'])
        return value


def _relations(backend,op,deadline):
    budget = _BudgetBackend(backend);indices = []
    for kind in p.KINDS:
        pages = p.scan_relation(budget,op,kind,deadline=deadline)
        after = None
        for number,page in enumerate(pages):
            name = op.phase+'-preservation-'+kind+'-%04d.json'%number
            hashes = op.journal.output_hashes();need(name in hashes,'preservation_page_not_journaled')
            ref = _ref(name,hashes[name],relation=kind,page=number,rows=len(page['rows']))
            cursor = 'NULL' if after is None else "convert_from(decode('%s','hex'),'UTF8')::jsonb"%canonical(after).hex()
            sql = "SELECT openlegal_checkpoint.preservation_page('%s',%s,%d)"%(kind,cursor,p.PAGE_SIZE)
            actual = _bundle(decode(op.journal.read_phase_bytes(op.phase,name,ref['sha256'])),_identity(op),sql)
            need(actual == page,'preservation_page_result_binding');p.validate_page(kind,actual,after)
            indices.append(ref);after = page['next_key']
    return indices,budget.rows


def _reference_sql(offset):
    # Count the RAW capped observer before DISTINCT. Its200001 sentinel must
    # never disappear through deduplication and falsely prove a complete scan.
    return ("WITH source AS (SELECT openlegal_checkpoint.retained_references() AS refs), "
        "refs AS (SELECT DISTINCT value FROM source CROSS JOIN LATERAL "
        "jsonb_array_elements(source.refs) AS t(value)) "
        "SELECT jsonb_build_object('source_rows',(SELECT jsonb_array_length(refs) FROM source),"
        "'rows',COALESCE(jsonb_agg(value ORDER BY (value->>'role') COLLATE \"C\","
        "(value->>'storage_key') COLLATE \"C\"),'[]'::jsonb)) "
        "FROM (SELECT value FROM refs ORDER BY (value->>'role') COLLATE \"C\","
        "(value->>'storage_key') COLLATE \"C\" OFFSET %d LIMIT 128) q")%offset


def _reference_result(value,offset):
    need(type(value) is dict and set(value) == {'source_rows','rows'}
         and type(value['source_rows']) is int and 0 <= value['source_rows'] <= p.MAX_ROWS
         and type(value['rows']) is list and len(value['rows']) <= CHUNK
         and offset+len(value['rows']) <= value['source_rows'],'preservation_reference_source_bound')
    return value['rows'],value['source_rows']


def _reference_rows(rows,last=None):
    need(type(rows) is list and len(rows) <= CHUNK,'preservation_reference_page')
    for row in rows:
        need(type(row) is dict and set(row) == {'role','storage_key','sha256','size'}
             and row['role'] in ('cache','corpus'),'preservation_reference_shape')
        generation = {k:v for k,v in row.items() if k != 'role'}
        p.compare_generations([generation],[generation])
        key = (row['role'],row['storage_key'])
        need(last is None or key > last,'preservation_reference_order_or_conflict');last = key
    return last


def _references(backend,op,deadline):
    rows = [];refs = [];last = None;source_rows = None
    while True:
        _deadline(deadline)
        need(len(refs) < p.MAX_PAGES,'preservation_reference_pages_bound')
        value,ref = _query(backend,op,_reference_sql(len(rows)),
                         'capture-references-%04d'%len(refs),deadline)
        page,count = _reference_result(value,len(rows))
        need(source_rows is None or count == source_rows,'preservation_reference_source_changed')
        source_rows = count
        last = _reference_rows(page,last)
        need(len(rows)+len(page) <= p.MAX_ROWS,'preservation_reference_rows_bound')
        refs.append(dict(ref,offset=len(rows),rows=len(page)));rows.extend(page)
        if len(page) < CHUNK:break
    return refs,rows,source_rows


def _root_rows(op):
    rows = op.inventory['linux']['bound_roots']
    need(type(rows) is list and len(rows) == 4 and {r.get('role') for r in rows} == set(ROLES),
         'preservation_four_blob_roots')
    result = {}
    for row in rows:
        need(type(row) is dict and set(row) == {'role','path','uid','dev','ino'} and row['uid'] == 10004
             and type(row['path']) is str and Path(row['path']).is_absolute()
             and '..' not in Path(row['path']).parts and str(Path(row['path'])) == row['path']
             and all(type(row[k]) is int and row[k] >= 0 for k in ('uid','dev','ino')),
             'preservation_blob_root_pin')
        result[row['role']] = copy.deepcopy(row)
    need(len({r['path'] for r in rows}) == 4 and len({(r['dev'],r['ino']) for r in rows}) == 4,
         'preservation_distinct_blob_roots')
    return result


def _physical(backend,op,references,deadline):
    result = []
    for role,root in _root_rows(op).items():
        op.seal();_deadline(deadline)
        actual = bootstrap.blob_inventory(root['path'],deadline=deadline,controls=True,online=False)
        need(actual['root_identity'] == {k:root[k] for k in ('dev','ino')}
             and actual['permanent_sha256'] == digest(canonical(actual['permanent'])),
             'preservation_blob_root_changed')
        p.compare_generations(actual['permanent'],actual['permanent'])
        selected = [r for r in references if r['role'] == role.split('_')[0]]
        for start in range(0,len(selected),CHUNK):
            _deadline(deadline);backend._verify_references(Path(root['path']),selected[start:start+CHUNK],deadline=deadline)
            _deadline(deadline)
        chunks = []
        for start in range(0,max(1,len(actual['permanent'])),CHUNK):
            chunk = actual['permanent'][start:start+CHUNK]
            ref = _saved(backend,op,'capture-generations-'+role+'-%04d'%len(chunks),{
                'schema_version':1,'kind':'openlegal_preservation_generation_chunk',**_identity(op),
                'role':role,'root':root,'offset':start,'rows':chunk})
            chunks.append(dict(ref,offset=start,rows=len(chunk)))
        result.append({'role':role,'root':root,'chunks':chunks,'generations':len(actual['permanent']),
            'permanent_sha256':actual['permanent_sha256'],'regular_bytes':actual['regular_bytes'],
            'private_witness_sha256':actual['private_witness_sha256'],
            'health':actual['health'],'controls':actual['controls'],
            'references_verified':len(selected)})
    return result


class _Stored:
    """Read only strict derived phase paths, with both copies and inode pins."""
    def __init__(self,op,reference):
        need(type(reference) is dict and set(reference) == REF_FIELDS
             and type(reference['schema_version']) is int and reference['schema_version'] == 1
             and reference['kind'] == 'openlegal_preservation_reference'
             and valid_uuid(reference['operation_id']) and reference['phase'] in (
                 'preservation_baseline','preservation_final') and reference['name'] == INDEX_NAME
             and pin(reference['sha256']),'preservation_stored_reference')
        self.op = op;self.reference = reference;self.contexts = reference['journal_phase_roots'];self.seen = {}
        roots = op.inventory['journal_roots']
        need(type(self.contexts) is list and len(self.contexts) == len(roots) == 2
             and roots[0]['identity']['dev'] != roots[1]['identity']['dev'],
             'preservation_stored_two_journals')
        self.check()
    def check(self):
        for root,context in zip(self.op.inventory['journal_roots'],self.contexts):
            need(type(context) is dict and set(context) == {'path','identity','operation_identity'},
                 'preservation_stored_phase_root')
            need(all(type(context[k]) is dict and set(context[k]) == {'dev','ino'}
                     and all(type(v) is int and v >= 0 for v in context[k].values())
                     for k in ('identity','operation_identity')),'preservation_stored_directory_pin')
            base = Path(root['path']);directory(base,root['identity'])
            operation = base/self.reference['operation_id'];directory(operation,context['operation_identity'])
            wanted = operation/self.reference['phase']
            need(context['path'] == str(wanted),'preservation_stored_path_not_derived')
            directory(wanted,context['identity'])
    def read(self,ref):
        need(type(ref) is dict and type(ref.get('name')) is str and NAME.fullmatch(ref['name'])
             and pin(ref.get('sha256')),'preservation_stored_artifact')
        need(ref['name'] not in self.seen or self.seen[ref['name']] == ref['sha256'],
             'preservation_conflicting_artifact_pins')
        self.seen[ref['name']] = ref['sha256']
        self.check();copies = []
        for context in self.contexts:
            path = Path(context['path']);fd = os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
            try:
                actual = os.fstat(fd)
                need((actual.st_dev,actual.st_ino) == (context['identity']['dev'],context['identity']['ino']),
                     'preservation_stored_parent_rebound')
                raw = private_bytes(path/ref['name'],ref['sha256'],limit=MAX_INDEX)
                readback(fd,ref['name'],raw);copies.append(raw)
            finally:os.close(fd)
        self.check();need(copies[0] == copies[1],'preservation_stored_copies_differ')
        return decode(copies[0])
    def recheck(self,deadline):
        for name,sha in list(self.seen.items()):
            _deadline(deadline);self.read(_ref(name,sha))


def _capture(backend,op,deadline,final):
    _scope(op,final);_deadline(deadline);roots = _phase_roots(op)
    source = _saved(backend,op,'capture-source-phase',{
        'schema_version':1,'kind':'openlegal_preservation_source_phase',**_identity(op),
        'journal_phase_roots':roots,'source_graph':copy.deepcopy(op.inventory['source_graph']),
        'permitted':True,'producer_os_exit_zero_claimed':False})
    before_refs,before = _stage(backend,op,'before',deadline,final)
    pages,rows = _relations(backend,op,deadline)
    reference_pages,references,reference_source_rows = _references(backend,op,deadline)
    physical = _physical(backend,op,references,deadline)
    after_refs,after = _stage(backend,op,'after',deadline,final)
    _stable(before,after)
    need(len(pages)+len(reference_pages)+sum(len(r['chunks']) for r in physical) <= p.MAX_PAGES,
         'preservation_global_chunk_bound')
    op.seal();_deadline(deadline);need(_phase_roots(op) == roots,'preservation_phase_root_changed')
    return {'schema_version':1,'kind':'openlegal_preservation_capture_index',**_identity(op),
        'journal_phase_roots':roots,'source_phase':source,'stage_proofs':{'before':before_refs,'after':after_refs},
        'relation_pages':pages,'rows':rows,'reference_pages':reference_pages,'references':len(references),'reference_source_rows':reference_source_rows,
        'physical_roots':physical,'before':before,'after':after,
        'producer_os_exit_zero_claimed':False,'public_acknowledgement_claimed':False,
        'checkpoint_claimed':False,'final_acceptance_claimed':False}


def _publish(backend,op,index,deadline):
    _deadline(deadline)
    raw = canonical(index);need(len(raw) <= MAX_INDEX,'preservation_index_bound')
    sha = op.journal.save(INDEX_NAME,index)
    need(sha == digest(raw) and op.journal.read_phase_bytes(op.phase,INDEX_NAME,sha) == raw,
         'preservation_index_dual_ack')
    _deadline(deadline)
    backend._artifacts[INDEX_NAME] = sha
    reference = {'schema_version':1,'kind':'openlegal_preservation_reference','operation_id':op.operation_id,
        'phase':op.phase,'name':INDEX_NAME,'sha256':sha,'journal_phase_roots':index['journal_phase_roots']}
    return {'index':index,'reference':reference}


def capture_baseline(backend,op,*,deadline):
    """One immutable full baseline; caller publishes external pointer only later."""
    return _publish(backend,op,_capture(backend,op,deadline,False),deadline)


def load_capture(backend,op,reference,*,deadline):
    """Authenticate BOTH old media and every indexed source before returning rows."""
    _deadline(deadline);stored = _Stored(op,reference);index = stored.read(reference)
    fields = {'schema_version','kind',*_identity(op),'journal_phase_roots','source_phase','stage_proofs',
        'relation_pages','rows','reference_pages','references','reference_source_rows','physical_roots','before','after',
        'producer_os_exit_zero_claimed','public_acknowledgement_claimed','checkpoint_claimed','final_acceptance_claimed'}
    need(type(index) is dict and set(index) == fields and type(index['schema_version']) is int
         and index['schema_version'] == 1 and index['kind'] == 'openlegal_preservation_capture_index'
         and index['operation_id'] == reference['operation_id'] and index['phase'] == reference['phase']
         and index['action'] == ('strict_prepare' if index['phase'] == 'preservation_baseline' else 'canary')
         and index['journal_phase_roots'] == stored.contexts and pin(index['authority_sha256'])
         and all(index[k] == _identity(op)[k] for k in ('inventory_sha256','source_graph_sha256',
             'managed_epoch','target_container_id'))
         and all(index[k] is False for k in ('producer_os_exit_zero_claimed','public_acknowledgement_claimed',
             'checkpoint_claimed','final_acceptance_claimed')),'preservation_index_identity')
    identity = {k:index[k] for k in _identity(op)}
    source = stored.read(index['source_phase'])
    need(source == {'schema_version':1,'kind':'openlegal_preservation_source_phase',**identity,
         'journal_phase_roots':stored.contexts,'source_graph':op.inventory['source_graph'],
         'permitted':True,'producer_os_exit_zero_claimed':False},'preservation_source_phase_binding')
    stages = {}
    need(type(index['stage_proofs']) is dict and set(index['stage_proofs']) == {'before','after'},
         'preservation_stage_sequence')
    for stage in ('before','after'):
        refs = index['stage_proofs'][stage]
        need(type(refs) is dict and set(refs) == {'closure','provider','position','replica_prefix','replica_flush'},
             'preservation_stage_proof_shape')
        for key,suffix in (('closure','closure'),('provider','provider'),('position','position'),
                           ('replica_prefix','replica-prefix'),('replica_flush','replica-flush')):
            need(type(refs[key]) is dict and refs[key].get('name') == index['phase']+'-capture-'+stage+'-'+suffix+'.json',
                 'preservation_stage_artifact_sequence')
        closure = stored.read(refs['closure'])
        need(type(closure) is dict and set(closure) == {'schema_version','kind',*identity,
                'result','failed','actual_command_evidence'}
             and type(closure['schema_version']) is int and closure['schema_version'] == 1
             and closure.get('kind') == 'openlegal_preservation_physical_closure' and closure.get('failed') is False
             and all(closure.get(k) == identity[k] for k in identity)
             and type(closure.get('result')) is dict and set(closure['result']) == provider.CLOSURE_FIELDS
             and closure['result']['sql_sessions'] == [],'preservation_stored_closure')
        _witness(closure['actual_command_evidence'],'SELECT openlegal_checkpoint.client_sessions()',{'sessions':[]})
        held = _bundle(stored.read(refs['provider']),identity,
                       "SELECT openlegal_checkpoint.observe('%s'::uuid)"%index['operation_id'])
        projection = provider._project(held)
        need(projection['clean'] and projection['recovery_hold'] and held['state']['admissions'] == []
             and held['admission_ownership']['slots'] == [] and (index['phase'] == 'preservation_baseline'
                 or projection['owner_operation_id'] == index['operation_id']),'preservation_stored_provider')
        position = _position(_bundle(stored.read(refs['position']),identity,
                                   'SELECT openlegal_checkpoint.database_position()'))
        first = _bundle(stored.read(refs['replica_prefix']),identity,'SELECT openlegal_replication.capture_prefix()')
        second = _bundle(stored.read(refs['replica_flush']),identity,'SELECT openlegal_replication.observe_flush()')
        _replica(backend,op,first,second,position)
        stages[stage] = {'provider':projection,'position':position,'replica':second,'closure':closure['result']}
        need(index[stage] == stages[stage],'preservation_stage_summary_binding')
    _stable(stages['before'],stages['after'])
    pages = {kind:[] for kind in p.KINDS};rows = 0;refs = index['relation_pages']
    need(type(refs) is list and len(refs) <= p.MAX_PAGES,'preservation_stored_page_bound')
    cursor = 0
    for kind in p.KINDS:
        after = None
        while True:
            _deadline(deadline);need(cursor < len(refs),'preservation_stored_missing_relation')
            ref = refs[cursor];number = len(pages[kind])
            need(set(ref) == {'name','sha256','relation','page','rows'} and ref['relation'] == kind
                 and type(ref['page']) is int and ref['page'] == number
                 and ref['name'] == index['phase']+'-preservation-'+kind+'-%04d.json'%number,
                 'preservation_stored_page_sequence')
            after_sql = 'NULL' if after is None else "convert_from(decode('%s','hex'),'UTF8')::jsonb"%canonical(after).hex()
            sql = "SELECT openlegal_checkpoint.preservation_page('%s',%s,%d)"%(kind,after_sql,p.PAGE_SIZE)
            page = _bundle(stored.read(ref),identity,sql);p.validate_page(kind,page,after)
            need(type(ref['rows']) is int and ref['rows'] == len(page['rows']),'preservation_page_count_binding')
            rows += len(page['rows']);need(rows <= p.MAX_ROWS,'preservation_global_row_bound')
            pages[kind].append(page);cursor += 1
            if not page['has_more']:break
            after = page['next_key']
    need(cursor == len(refs) and type(index['rows']) is int and index['rows'] == rows,
         'preservation_stored_extra_pages')
    references = [];last = None;refpages = index['reference_pages']
    need(type(refpages) is list and 1 <= len(refpages) <= p.MAX_PAGES,'preservation_stored_reference_pages')
    for n,ref in enumerate(refpages):
        _deadline(deadline)
        need(set(ref) == {'name','sha256','offset','rows'} and type(ref['offset']) is int
             and ref['offset'] == len(references)
             and ref['name'] == index['phase']+'-capture-references-%04d.json'%n,
             'preservation_stored_reference_sequence')
        value = _bundle(stored.read(ref),identity,_reference_sql(len(references)))
        actual,count = _reference_result(value,len(references))
        need(type(index['reference_source_rows']) is int and count == index['reference_source_rows'],
             'preservation_reference_source_changed')
        last = _reference_rows(actual,last)
        need(type(ref['rows']) is int and ref['rows'] == len(actual)
             and (len(actual) == CHUNK) == (n+1 < len(refpages)),'preservation_stored_reference_terminal')
        references.extend(actual);need(len(references) <= p.MAX_ROWS,'preservation_reference_rows_bound')
    need(type(index['references']) is int and index['references'] == len(references),
         'preservation_stored_reference_count')
    physical = {};expected_roots = _root_rows(op)
    need(type(index['physical_roots']) is list and len(index['physical_roots']) == 4,'preservation_stored_roots')
    total_chunks = len(refs)+len(refpages)
    for record in index['physical_roots']:
        need(type(record) is dict and set(record) == {'role','root','chunks','generations','permanent_sha256',
            'regular_bytes','private_witness_sha256','health','controls','references_verified'}
            and all(type(record[k]) is int and 0 <= record[k] <= p.MAX_ROWS for k in ('generations','references_verified'))
            and type(record['regular_bytes']) is int and 0 <= record['regular_bytes'] <= bootstrap.MAX_BYTES
            and pin(record['permanent_sha256']) and pin(record['private_witness_sha256'])
            and type(record['health']) is list and len(record['health']) <= 8
            and type(record['controls']) is list and len(record['controls']) <= 514,
            'preservation_stored_root_shape')
        role = record.get('role');need(role in expected_roots and role not in physical
             and record.get('root') == expected_roots[role],'preservation_stored_root_binding')
        chunks = record['chunks'];need(type(chunks) is list and 1 <= len(chunks) <= p.MAX_PAGES,
                                      'preservation_generation_chunks_bound')
        generations = []
        for n,ref in enumerate(chunks):
            _deadline(deadline)
            need(set(ref) == {'name','sha256','offset','rows'} and type(ref['offset']) is int
                 and ref['offset'] == len(generations)
                 and ref['name'] == index['phase']+'-capture-generations-'+role+'-%04d.json'%n,
                 'preservation_generation_chunk_sequence')
            chunk = stored.read(ref)
            need(type(chunk) is dict and set(chunk) == {'schema_version','kind',*identity,'role','root','offset','rows'}
                 and type(chunk['schema_version']) is int and chunk['schema_version'] == 1
                 and chunk['kind'] == 'openlegal_preservation_generation_chunk'
                 and all(chunk[k] == identity[k] for k in identity) and chunk['root'] == expected_roots[role]
                 and chunk['role'] == role and chunk['offset'] == len(generations)
                 and type(chunk['rows']) is list and len(chunk['rows']) <= CHUNK
                 and type(ref['rows']) is int and ref['rows'] == len(chunk['rows'])
                 and (n+1 == len(chunks) or len(chunk['rows']) == CHUNK),
                 'preservation_generation_chunk_binding')
            need(chunk['rows'] or len(chunks) == 1,'preservation_empty_generation_continuation')
            last = generations[-1]['storage_key'] if generations else None
            for row in chunk['rows']:
                need(type(row) is dict and type(row.get('storage_key')) is str
                     and (last is None or row['storage_key'] > last),'preservation_generation_chunk_order')
                last = row['storage_key']
            generations.extend(chunk['rows']);need(len(generations) <= p.MAX_ROWS,'preservation_generation_bound')
        p.compare_generations(generations,generations)
        need(record['generations'] == len(generations)
             and record['permanent_sha256'] == digest(canonical(generations))
             and record['regular_bytes'] == sum(r['size'] for r in generations)
             and record['references_verified'] == sum(r['role'] == role.split('_')[0] for r in references),
             'preservation_generation_summary_binding')
        physical[role] = generations;total_chunks += len(chunks)
    need(total_chunks <= p.MAX_PAGES,'preservation_global_chunk_bound')
    stored.recheck(deadline);stored.check();_deadline(deadline)
    return {'index':index,'pages':pages,'physical':physical,'references':references}


def capture_final(backend,op,baseline_reference,*,deadline):
    """Before M31 finish, retain every original row/generation under owned hold."""
    _scope(op,True)
    need(type(baseline_reference) is dict and baseline_reference.get('phase') == 'preservation_baseline','preservation_original_baseline_required')
    baseline = load_capture(backend,op,baseline_reference,deadline=deadline)
    index = _capture(backend,op,deadline,True)
    # This immutable candidate has no subset-success claim. A separate result
    # is acknowledged only after all original subsets have actually passed.
    temporary = _publish(backend,op,index,deadline)
    current = load_capture(backend,op,temporary['reference'],deadline=deadline)
    # Authenticate original immutable bytes again after the potentially long
    # final physical scan; no old-media fallback or historical-phase adoption.
    baseline = load_capture(backend,op,baseline_reference,deadline=deadline)
    a,b = baseline['index']['after']['position'],index['before']['position']
    need(all(a[k] == b[k] for k in ('system_identifier','timeline'))
         and all(b[k] >= a[k] for k in ('watermark','index_ack'))
         and _lsn(b['wal_lsn']) >= _lsn(a['wal_lsn']),'preservation_baseline_database_changed')
    comparisons = []
    for kind in p.KINDS:
        _deadline(deadline)
        comparisons.append(p.compare_subset(kind,baseline['pages'][kind],current['pages'][kind]))
        _deadline(deadline)
    generations = {}
    for role in ROLES:
        _deadline(deadline)
        generations[role] = p.compare_generations(baseline['physical'][role],current['physical'][role])
        _deadline(deadline)
    result = {'schema_version':1,'kind':'openlegal_preservation_subset_result',**_identity(op),
        'baseline':baseline_reference,'final':temporary['reference'],'relations':comparisons,'generations':generations,
        'every_baseline_row_unchanged':True,'all_baseline_generations_preserved':True,
        'public_acknowledgement_claimed':False,'checkpoint_claimed':False,'final_acceptance_claimed':False}
    _deadline(deadline)
    temporary['comparison'] = _saved(backend,op,'capture-subset-result',result)
    _deadline(deadline)
    return temporary
