"""Quiet, chunked permanent-history evidence for the explicit canary.

Imports are inert. SQL is fixed and metadata-only; pages and their actual command
evidence are acknowledged through BOTH journals before being accepted. Complete
baseline/final scans require separately proven physical writer closure. Neither
row counts nor a new candidate establish an original producer acknowledgement.
"""
from __future__ import annotations

import copy
import re
import time
from types import SimpleNamespace

from interface import Refused, need
from inventory import canonical, decode, digest, pin, valid_uuid

KINDS = ('corpus_capture','corpus_capture_catalog','corpus_capture_blob',
    'corpus_source_observation','corpus_object_identity','corpus_revision_identity',
    'cache_snapshot','cache_query_identity','cache_blob_identity',
    'current_inventory_audit','provider_recovery_audit','provider_canary_audit',
    'checkpoint_operation','canary_lifecycle','provider_canary')
PAGE_SIZE = 128
MAX_ROWS = 200000
MAX_PAGES = 4096
MAX_PAGE_BYTES = 256*1024
MAX_BUNDLE_BYTES = 2*1024**2
HASH = r'[0-9a-f]{64}'
UUID = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
KEY_TYPES = {kind:(HASH,) for kind in KINDS}
KEY_TYPES.update({kind:(UUID,) for kind in ('cache_snapshot','cache_query_identity',
    'current_inventory_audit','provider_recovery_audit','checkpoint_operation',
    'canary_lifecycle','provider_canary')})
KEY_TYPES.update(corpus_capture_blob=(HASH,r'0|-?[1-9][0-9]{0,9}'),
    corpus_revision_identity=(HASH,HASH),provider_canary_audit=(UUID,UUID,r'init|close'))
# Each field is (nullable, maximum). These are the exact M32 projections.
U64 = 18446744073709551615
I64 = 9223372036854775807
MONOTONIC = {
    'corpus_source_observation':{'validated_at':(False,U64)},
    'corpus_object_identity':{'version':(False,I64),'catalog_version':(False,I64),
        'next_capture':(False,I64),'validated_at':(True,U64)},
    'corpus_revision_identity':{'last_sequence':(False,I64),'captured_at':(True,U64),
        'last_validated_at':(True,U64)},
    'cache_query_identity':{'next_sequence':(False,I64),'mutation_revision':(False,I64)}}


def integer(value, code='preservation_integer'):
    need(type(value) is str and re.fullmatch(r'0|[1-9][0-9]{0,19}',value) is not None,code)
    result = int(value)
    need(result <= 18446744073709551615,code)
    return result


def row_key(kind, key):
    need(kind in KINDS and type(key) is list and len(key) == len(KEY_TYPES[kind])
         and all(type(part) is str and re.fullmatch(pattern,part) is not None
                 for part,pattern in zip(key,KEY_TYPES[kind])),'preservation_row_key')
    # Only this fixed primary-key position is numeric. Other key components
    # use the observer's deterministic C ordering, not locale-dependent order.
    if kind == 'corpus_capture_blob':
        need(len(key) == 2,'preservation_blob_key')
        number = int(key[1]);need(-2147483648 <= number <= 2147483647,'preservation_blob_ordinal')
        return (key[0],number)
    return tuple(key)


def validate_page(kind, value, after=None, *, limit=PAGE_SIZE):
    need(kind in KINDS and type(limit) is int and 1 <= limit <= PAGE_SIZE,
         'preservation_page_request')
    need(type(value) is dict and set(value) == {'schema_version','kind','relation','rows','next_key','has_more'}
         and type(value['schema_version']) is int and value['schema_version'] == 1
         and value['kind'] == 'openlegal_preservation_page' and value['relation'] == kind
         and type(value['rows']) is list and len(value['rows']) <= limit
         and type(value['has_more']) is bool and len(canonical(value)) <= MAX_PAGE_BYTES,
         'preservation_page_shape')
    last = row_key(kind,after) if after is not None else None
    for row in value['rows']:
        need(type(row) is dict and set(row) == {'key','sha256','monotonic'}
             and pin(row['sha256']),
             'preservation_row_shape')
        schema = MONOTONIC.get(kind)
        if schema is None:
            need(row['monotonic'] is None,'preservation_monotonic_schema')
        else:
            need(type(row['monotonic']) is dict and set(row['monotonic']) == set(schema),
                 'preservation_monotonic_schema')
        current = row_key(kind,row['key'])
        need(last is None or current > last,'preservation_page_order_or_duplicate')
        for name,number in (row['monotonic'] or {}).items():
            nullable,maximum = schema[name]
            need(number is not None or nullable,'preservation_monotonic_schema')
            if number is not None:
                need(integer(number) <= maximum,'preservation_counter_domain')
                if name in ('next_capture','next_sequence'):
                    need(integer(number) >= 1,'preservation_counter_domain')
        last = current
    if value['has_more']:
        need(len(value['rows']) == limit and value['next_key'] == value['rows'][-1]['key'],
             'preservation_continuation')
    else:
        need(value['next_key'] is None,'preservation_final_cursor')
    return value


def compare_subset(kind, baseline_pages, final_pages):
    """Every old immutable row survives, with explicit monotonic fields only."""
    def collect(pages):
        need(type(pages) is list and 1 <= len(pages) <= MAX_PAGES,'preservation_pages_bound')
        result = {};after = None
        for index,page in enumerate(pages):
            validate_page(kind,page,after)
            need(page['has_more'] == (index+1 < len(pages)), 'preservation_page_chain')
            for row in page['rows']:
                key = row_key(kind,row['key'])
                need(key not in result and len(result) < MAX_ROWS,'preservation_rows_bound')
                result[key] = row
            after = page['next_key']
        return result
    before,after = collect(baseline_pages),collect(final_pages)
    for key,row in before.items():
        need(key in after,'preservation_old_row_missing')
        current = after[key]
        need(current['sha256'] == row['sha256']
             and type(current['monotonic']) is type(row['monotonic'])
             and set(current['monotonic'] or {}) == set(row['monotonic'] or {}),
             'preservation_old_row_changed')
        for name,value in (row['monotonic'] or {}).items():
            if value is not None:
                need(current['monotonic'][name] is not None
                     and integer(current['monotonic'][name]) >= integer(value),
                     'preservation_counter_regressed')
    return {'relation':kind,'baseline_rows':len(before),'final_rows':len(after),
            'every_baseline_row_unchanged':True,'monotonic_fields_preserved':True}


def compare_generations(baseline, final):
    """Never excuse removal of an old ready or unreferenced generation."""
    def collect(rows):
        need(type(rows) is list and len(rows) <= MAX_ROWS,'preservation_generation_bound')
        result = {}
        for row in rows:
            need(type(row) is dict and set(row) == {'storage_key','sha256','size'}
                 and type(row['storage_key']) is str
                 and pin(row['sha256']) and type(row['size']) is int and 0 <= row['size'] <= 100*1024**2,
                 'preservation_generation_shape')
            match = re.fullmatch(r'([0-9a-f]{2})/('+HASH+r')-('+UUID+r')',row['storage_key'])
            need(match is not None and match[1] == row['sha256'][:2] and match[2] == row['sha256']
                 and valid_uuid(match[3]),'preservation_generation_key')
            need(row['storage_key'] not in result,'preservation_duplicate_generation')
            result[row['storage_key']] = row
        return result
    before,after = collect(baseline),collect(final)
    need(all(key in after and row == after[key] for key,row in before.items()),
         'preservation_old_generation_missing_or_changed')
    return {'baseline_generations':len(before),'final_generations':len(after),
            'all_baseline_generations_preserved':True}


class CommandBuffer:
    """Temporary evidence only; the caller must durably save the whole bundle."""
    def __init__(self):self.values = {};self.bytes = 0
    def save(self,name,value):
        raw = canonical(value)
        need(type(name) is str and re.fullmatch('[A-Za-z0-9_.-]{1,127}',name)
             and name not in self.values and len(self.values) < 128
             and self.bytes+len(raw) <= MAX_BUNDLE_BYTES,'preservation_command_evidence_bound')
        self.values[name] = {'sha256':digest(raw),'value':copy.deepcopy(value)}
        self.bytes += len(raw)
        return digest(raw)


def _query_bundle(backend,op,sql,name,deadline):
    """One fixed read, with every underlying CLI result preserved together."""
    need(time.monotonic() < deadline,'preservation_scan_deadline')
    target = op.inventory['linux']['postgres']['primary']
    need(valid_uuid(op.operation_id) and valid_uuid(op.inventory['managed_epoch'])
         and all(pin(value) for value in (op.inventory_sha256,op.source_graph_sha256,op.authority_sha256))
         and pin(target['container_id']),'preservation_query_subject')
    buffer = CommandBuffer()
    fields = {key:getattr(op,key) for key in ('operation_id','action','phase','inventory',
        'inventory_sha256','source_graph_sha256','authority_sha256','authority','previous','seal')}
    query_op = SimpleNamespace(**fields,journal=buffer)
    old = dict(backend._artifacts)
    result = None;failed = False
    try:
        result = backend._sql(query_op,sql,deadline=deadline)
        from linux_backend import SQL_SESSION_PREFIX
        extra = "SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; "
        inputs = {digest((SQL_SESSION_PREFIX+suffix+sql+';\n').encode()) for suffix in ('',extra)}
        witnessed = False
        for record in buffer.values.values():
            value = record['value']
            need(record['sha256'] == digest(canonical(value)), 'preservation_command_evidence_hash')
            if not (type(value) is dict and value.get('binary') == 'docker'
                    and value.get('stdin_sha256') in inputs):continue
            need(type(value.get('actual_exit_code')) is int and value['actual_exit_code'] == 0
                 and value.get('started') is True and value.get('owned_processes_closed') is True
                 and all(value.get(key) is False for key in ('expired','output_limit_exceeded',
                     'unexpected_descendants','interrupted')),'preservation_query_command_not_closed')
            raw = value.get('stdout_hex')
            need(type(raw) is str and len(raw) <= MAX_BUNDLE_BYTES,'preservation_query_output_witness')
            try:
                data = bytes.fromhex(raw)
                parsed = decode(data)
            except (ValueError,UnicodeError,RecursionError):
                raise Refused('preservation_query_output_witness') from None
            need(len(data) == value.get('stdout_bytes') and digest(data) == value.get('stdout_sha256')
                 and parsed == result,'preservation_query_output_witness')
            witnessed = True
        need(witnessed,'preservation_actual_command_evidence_required')
        return result
    except BaseException:
        failed = True
        raise
    finally:
        backend._artifacts.clear();backend._artifacts.update(old)
        # A second-media error is fatal even if the read itself succeeded. No
        # unjournalled metadata or merely in-memory command ACK is accepted.
        backend._artifact(op,name,{'schema_version':1,'kind':'openlegal_preservation_query_bundle',
            'operation_id':op.operation_id,'phase':op.phase,'managed_epoch':op.inventory['managed_epoch'],
            'inventory_sha256':op.inventory_sha256,'source_graph_sha256':op.source_graph_sha256,
            'authority_sha256':op.authority_sha256,'target_container_id':target['container_id'],
            'query_sha256':digest(sql.encode()),'result':result,'failed':failed,
            'actual_command_evidence':buffer.values})


def scan_relation(backend,op,kind,*,deadline):
    need(kind in KINDS,'preservation_relation_whitelist')
    pages = [];after = None;count = 0
    while True:
        op.seal();need(len(pages) < MAX_PAGES,'preservation_pages_bound')
        cursor = 'NULL' if after is None else "convert_from(decode('%s','hex'),'UTF8')::jsonb"%canonical(after).hex()
        sql = "SELECT openlegal_checkpoint.preservation_page('%s',%s,%d)"%(kind,cursor,PAGE_SIZE)
        page = _query_bundle(backend,op,sql,'preservation-'+kind+'-%04d'%len(pages),deadline)
        validate_page(kind,page,after)
        count += len(page['rows']);need(count <= MAX_ROWS,'preservation_rows_bound')
        pages.append(page)
        if not page['has_more']:break
        after = page['next_key']
    return pages
