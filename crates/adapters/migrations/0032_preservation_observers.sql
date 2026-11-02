-- Fixed private metadata observers. No runtime setting, role, credential,
-- publication, provider request, body disclosure or authority is created here.
-- Page consistency is a CALLER precondition: all writers physically closed,
-- stable DB/source identity and actual prior exits/closed dual journals.
-- Keys are arrays of canonical text PK components. Text uses fixed C collation;
-- UUID/bytea/integer use native order (ordinal text must compare numerically).
-- Revision-ID cursor components are SHA256(UTF8 revision), never raw text.
-- Cursor is opaque; duplicate hashed keys, including page boundaries, refuse.
-- Full row hashes include bodies internally; only hashes/metadata leave SQL.
-- Monotonic values are exact decimal strings or null. Final >= baseline;
-- nonnull baseline may not become null. Audit rows have FULL hashes, no waivers.
CREATE FUNCTION openlegal_checkpoint.preservation_page(
 p_kind text,p_after jsonb,p_limit integer) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
 SET TimeZone='UTC' SET DateStyle='ISO,YMD' SET IntervalStyle='iso_8601'
 SET bytea_output='hex' SET extra_float_digits=3 AS $$
DECLARE relation_name text; key_count integer; key_patterns text[]; key_index integer;
 key_sql text; after_sql text;
 order_sql text; projection_sql text; monotonic_sql text; predicate_sql text;
 query text; item record; rows jsonb:='[]'::jsonb; count integer:=0;
 more boolean:=false; last_key jsonb; result jsonb;
BEGIN
 IF p_limit IS NULL OR p_limit<1 OR p_limit>128 THEN
  RAISE EXCEPTION 'preservation page bound' USING ERRCODE='22023'; END IF;
 CASE p_kind
  WHEN 'corpus_capture' THEN
   relation_name:='openlegal.corpus_capture'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{64}$'];
   key_sql:='jsonb_build_array(t.id::text)';
   after_sql:='t.id COLLATE "C" > ($1->>0)::text COLLATE "C"'; order_sql:='t.id COLLATE "C"';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='true';
  WHEN 'corpus_capture_catalog' THEN
   relation_name:='openlegal.corpus_capture_catalog'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{64}$'];
   key_sql:='jsonb_build_array(t.id::text)';
   after_sql:='t.id COLLATE "C" > ($1->>0)::text COLLATE "C"'; order_sql:='t.id COLLATE "C"';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='true';
  WHEN 'corpus_capture_blob' THEN
   relation_name:='openlegal.corpus_capture_blob'; key_count:=2;
   key_patterns:=ARRAY['^[0-9a-f]{64}$','^-?(0|[1-9][0-9]{0,9})$'];
   key_sql:='jsonb_build_array(t.capture_id::text,t.ordinal::text)';
   after_sql:='(t.capture_id COLLATE "C",t.ordinal) > (($1->>0)::text COLLATE "C",($1->>1)::integer)'; order_sql:='t.capture_id COLLATE "C",t.ordinal';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='true';
  WHEN 'corpus_source_observation' THEN
   relation_name:='openlegal.corpus_source_observation'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{64}$'];
   key_sql:='jsonb_build_array(t.id::text)';
   after_sql:='t.id COLLATE "C" > ($1->>0)::text COLLATE "C"'; order_sql:='t.id COLLATE "C"';
   projection_sql:='to_jsonb(t)-''validated_at''';
   monotonic_sql:='jsonb_build_object(''validated_at'',t.validated_at::text)'; predicate_sql:='true';
  WHEN 'corpus_object_identity' THEN
   relation_name:='openlegal.corpus_object'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{64}$'];
   key_sql:='jsonb_build_array(t.object_key::text)';
   after_sql:='t.object_key COLLATE "C" > ($1->>0)::text COLLATE "C"'; order_sql:='t.object_key COLLATE "C"';
   projection_sql:='jsonb_build_object(''object_key'',t.object_key,''identity'',t.identity)';
   monotonic_sql:='jsonb_build_object(''version'',t.version::text,''catalog_version'',t.catalog_version::text,''next_capture'',t.next_capture::text,''validated_at'',t.validated_at::text)'; predicate_sql:='true';
  WHEN 'corpus_revision_identity' THEN
   relation_name:='openlegal.corpus_revision'; key_count:=2;
   key_patterns:=ARRAY['^[0-9a-f]{64}$','^[0-9a-f]{64}$'];
   key_sql:='jsonb_build_array(t.object_key::text,encode(sha256(convert_to(t.revision_id,''UTF8'')),''hex''))';
   after_sql:='(t.object_key COLLATE "C",encode(sha256(convert_to(t.revision_id,''UTF8'')),''hex'') COLLATE "C") > (($1->>0)::text COLLATE "C",($1->>1)::text COLLATE "C")'; order_sql:='t.object_key COLLATE "C",encode(sha256(convert_to(t.revision_id,''UTF8'')),''hex'') COLLATE "C"';
   projection_sql:='jsonb_build_object(''object_key'',t.object_key,''revision_id'',t.revision_id)';
   monotonic_sql:='jsonb_build_object(''last_sequence'',t.last_sequence::text,''captured_at'',t.captured_at::text,''last_validated_at'',t.last_validated_at::text)'; predicate_sql:='true';
  WHEN 'cache_snapshot' THEN
   relation_name:='openlegal.cache_snapshot'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'];
   key_sql:='jsonb_build_array(t.id::text)';
   after_sql:='t.id > ($1->>0)::uuid'; order_sql:='t.id';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='true';
  WHEN 'cache_query_identity' THEN
   relation_name:='openlegal.cache_query'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'];
   key_sql:='jsonb_build_array(t.id::text)';
   after_sql:='t.id > ($1->>0)::uuid'; order_sql:='t.id';
   projection_sql:='to_jsonb(t)-''next_sequence''-''mutation_revision''';
   monotonic_sql:='jsonb_build_object(''next_sequence'',t.next_sequence::text,''mutation_revision'',t.mutation_revision::text)'; predicate_sql:='true';
  WHEN 'cache_blob_identity' THEN
   relation_name:='openlegal.blob_object'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{64}$'];
   key_sql:='jsonb_build_array(encode(t.sha256,''hex''))';
   after_sql:='t.sha256 > decode($1->>0,''hex'')'; order_sql:='t.sha256';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='t.ready';
  WHEN 'current_inventory_audit' THEN
   relation_name:='openlegal.current_inventory_audit'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'];
   key_sql:='jsonb_build_array(t.operation_id::text)';
   after_sql:='t.operation_id > ($1->>0)::uuid'; order_sql:='t.operation_id';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='true';
  WHEN 'provider_recovery_audit' THEN
   relation_name:='openlegal_admin.provider_recovery_audit'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'];
   key_sql:='jsonb_build_array(t.operation_id::text)';
   after_sql:='t.operation_id > ($1->>0)::uuid'; order_sql:='t.operation_id';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='true';
  WHEN 'provider_canary_audit' THEN
   relation_name:='openlegal_admin.provider_canary_audit'; key_count:=3;
   key_patterns:=ARRAY['^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$','^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$','^(init|close)$'];
   key_sql:='jsonb_build_array(t.operation_id::text,t.canary_id::text,t.action::text)';
   after_sql:='(t.operation_id,t.canary_id,t.action COLLATE "C") > (($1->>0)::uuid,($1->>1)::uuid,($1->>2)::text COLLATE "C")'; order_sql:='t.operation_id,t.canary_id,t.action COLLATE "C"';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='true';
  WHEN 'checkpoint_operation' THEN
   relation_name:='openlegal_checkpoint.operation'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'];
   key_sql:='jsonb_build_array(t.operation_id::text)';
   after_sql:='t.operation_id > ($1->>0)::uuid'; order_sql:='t.operation_id';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='true';
  WHEN 'canary_lifecycle' THEN
   relation_name:='openlegal_checkpoint.canary_lifecycle'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'];
   key_sql:='jsonb_build_array(t.canary_id::text)';
   after_sql:='t.canary_id > ($1->>0)::uuid'; order_sql:='t.canary_id';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='true';
  WHEN 'provider_canary' THEN
   relation_name:='openlegal_admin.provider_canary'; key_count:=1;
   key_patterns:=ARRAY['^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'];
   key_sql:='jsonb_build_array(t.id::text)';
   after_sql:='t.id > ($1->>0)::uuid'; order_sql:='t.id';
   projection_sql:='to_jsonb(t)';
   monotonic_sql:='NULL::jsonb'; predicate_sql:='true';
  ELSE RAISE EXCEPTION 'unknown preservation relation' USING ERRCODE='22023';
 END CASE;
 IF p_after IS NOT NULL AND jsonb_typeof(p_after)='null' THEN p_after:=NULL; END IF;
 IF p_after IS NOT NULL THEN
  IF jsonb_typeof(p_after)<>'array' OR jsonb_array_length(p_after)<>key_count
   OR octet_length(p_after::text)>4096 OR EXISTS(
    SELECT 1 FROM jsonb_array_elements(p_after) value
     WHERE jsonb_typeof(value)<>'string' OR octet_length(value#>>'{}')>1024)
  THEN RAISE EXCEPTION 'invalid preservation cursor' USING ERRCODE='22023'; END IF;
  FOR key_index IN 1..key_count LOOP
   IF (p_after->>(key_index-1)) !~ key_patterns[key_index] THEN
    RAISE EXCEPTION 'invalid preservation key type' USING ERRCODE='22023'; END IF;
  END LOOP;
 END IF;
 -- Every SQL fragment above is a fixed source literal, never a caller table,
 -- identifier, expression or SQL fragment. Cursor/limit/domain use parameters.
 query:='SELECT '||key_sql||' AS key, encode(sha256('
  ||'convert_to(''openlegal-preservation-row-v1/''||$3,''UTF8'')||decode(''00'',''hex'')||'
  ||'convert_to(('||projection_sql||')::text,''UTF8'')),''hex'') AS sha256,'
  ||monotonic_sql||' AS monotonic FROM (SELECT t.* FROM '||relation_name
  ||' t WHERE ('||predicate_sql||') AND ($1 IS NULL OR ('||after_sql||')) ORDER BY '
  ||order_sql||' LIMIT ($2+1)) t ORDER BY '||order_sql;
 FOR item IN EXECUTE query USING p_after,p_limit,p_kind LOOP
  FOR key_index IN 1..key_count LOOP
   IF (item.key->>(key_index-1)) !~ key_patterns[key_index] THEN
    RAISE EXCEPTION 'invalid stored preservation key' USING ERRCODE='23514'; END IF;
  END LOOP;
  IF last_key IS NOT NULL AND item.key=last_key THEN
   RAISE EXCEPTION 'preservation key collision' USING ERRCODE='23514'; END IF;
  count:=count+1;
  IF count>p_limit THEN more:=true; EXIT; END IF;
  rows:=rows||jsonb_build_array(jsonb_build_object('key',item.key,'sha256',item.sha256,
                                                'monotonic',item.monotonic));
  last_key:=item.key;
 END LOOP;
 result:=jsonb_build_object('schema_version',1,'kind','openlegal_preservation_page',
  'relation',p_kind,'rows',rows,'next_key',CASE WHEN more THEN last_key ELSE NULL END,
  'has_more',more);
 IF octet_length(result::text)>262144 THEN
  RAISE EXCEPTION 'preservation output bound' USING ERRCODE='54000'; END IF;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION openlegal_checkpoint.preservation_page(text,jsonb,integer) FROM PUBLIC;

-- This observer identifies committed BODY candidates, not producer-call ACKs,
-- fresh provider HTTP causality, legal applicability or full clone completion.
-- Actual exact-capture public database.get, fresh replica fence, both physical
-- Blob copies and unchanged original-subset proof remain separate mandatory gates.
CREATE FUNCTION openlegal_checkpoint.publication_page(
 p_after_event bigint,p_after_id text,p_limit integer) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
 SET TimeZone='UTC' SET DateStyle='ISO,YMD' SET IntervalStyle='iso_8601'
 SET bytea_output='hex' SET extra_float_digits=3 AS $$
DECLARE item record; resources jsonb; primary_resource jsonb; refs jsonb;
 rows jsonb:='[]'::jsonb; count integer:=0; more boolean:=false;
 last_event bigint; last_id text; result jsonb; scanned integer:=0;
BEGIN
 IF p_after_event IS NULL OR p_after_event<0 OR p_limit IS NULL OR p_limit<1 OR p_limit>128
 OR (p_after_id IS NOT NULL AND p_after_id !~ '^[0-9a-f]{64}$') THEN
  RAISE EXCEPTION 'invalid publication cursor or bound' USING ERRCODE='22023'; END IF;
 FOR item IN
  SELECT c.*,o.identity,o.head_capture,o.pending,
   c.payload#>>'{record,metadata,original_resources}' AS resource_text
  FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key)
  JOIN openlegal.corpus_capture_catalog cat ON cat.id=c.id AND cat.object_key=c.object_key
   AND cat.revision_id=c.revision_id AND cat.sequence=c.sequence
  WHERE (c.event_sequence>p_after_event OR
    (p_after_id IS NOT NULL AND c.event_sequence=p_after_event AND c.id COLLATE "C">p_after_id COLLATE "C"))
   AND NOT o.withdrawn AND NOT o.current_inventory_conflict AND c.clone_body_complete
   AND o.identity->>'jurisdiction'='kr' AND o.identity->>'provider'='law_go_kr'
   AND jsonb_typeof(o.identity)='object' AND (SELECT count(*) FROM jsonb_object_keys(o.identity))=4
   AND o.identity ?& ARRAY['jurisdiction','provider','dataset','id']
   AND jsonb_typeof(o.identity->'dataset')='string' AND o.identity->>'dataset' ~ '^[a-z][a-z0-9_]{0,63}$'
   AND jsonb_typeof(o.identity->'id')='string' AND o.identity->>'id' ~ '^[A-Za-z0-9_-]{1,128}$'
   AND c.payload#>'{record,object}'=o.identity
   AND c.payload->>'capture_id'=c.id AND c.payload#>>'{record,revision_id}'=c.revision_id
   AND c.payload->>'raw_sha256'=encode(c.raw_sha256,'hex') AND c.raw_size>0
   AND jsonb_typeof(c.payload#>'{record,body}')='string'
   AND octet_length(c.payload#>>'{record,body}') BETWEEN 1 AND 16777216
   AND COALESCE(c.payload#>>'{record,metadata,attachment_status}','')<>'incomplete'
   AND COALESCE(c.payload#>>'{record,metadata,body_status}','') NOT IN
    ('response_identity_unverified_metadata_only','rights_unverified_metadata_only','provider_original_only')
   AND octet_length(c.payload#>>'{record,metadata,original_resources}') BETWEEN 1 AND 65536
  ORDER BY c.event_sequence,c.id COLLATE "C"
 LOOP
  scanned:=scanned+1;
  IF scanned>200000 THEN
   RAISE EXCEPTION 'publication scan bound' USING ERRCODE='54000'; END IF;
  -- Structural rights filters never create permission. Actual public read
  -- revalidates SourceRights::can_process, including full URL/Unicode semantics.
  BEGIN
   resources:=item.resource_text::jsonb;
  EXCEPTION WHEN invalid_text_representation THEN
   RAISE EXCEPTION 'invalid original resource metadata' USING ERRCODE='22023';
  END;
  IF jsonb_typeof(resources)<>'array' OR jsonb_array_length(resources)>65 THEN
   RAISE EXCEPTION 'invalid original resource metadata' USING ERRCODE='22023'; END IF;
  IF (SELECT count(*) FROM jsonb_array_elements(resources) r WHERE r->'ordinal'='0'::jsonb)<>1
  THEN CONTINUE; END IF;
  SELECT r INTO STRICT primary_resource FROM jsonb_array_elements(resources) r
   WHERE r->'ordinal'='0'::jsonb;
  IF primary_resource->'retained' IS DISTINCT FROM 'true'::jsonb
   OR primary_resource#>'{rights,redistribution}' IS DISTINCT FROM 'true'::jsonb
   OR primary_resource#>'{rights,derivatives}' IS DISTINCT FROM 'true'::jsonb
   OR jsonb_typeof(primary_resource#>'{rights,commercial_use}') IS DISTINCT FROM 'boolean'
   OR jsonb_typeof(primary_resource#>'{rights,basis}') IS DISTINCT FROM 'string'
   OR jsonb_typeof(primary_resource#>'{rights,attribution}') IS DISTINCT FROM 'string'
   OR jsonb_typeof(primary_resource#>'{rights,evidence_url}') IS DISTINCT FROM 'string'
   OR btrim(primary_resource#>>'{rights,basis}')=''
   OR btrim(primary_resource#>>'{rights,attribution}')=''
   OR btrim(primary_resource#>>'{rights,evidence_url}')=''
  THEN CONTINUE; END IF;
  count:=count+1;
  IF count>p_limit THEN more:=true; EXIT; END IF;
  SELECT jsonb_agg(jsonb_build_object('ordinal',b.ordinal,'sha256',encode(b.raw_sha256,'hex'),
    'size',b.raw_size::text,'storage_key',b.storage_key) ORDER BY b.ordinal) INTO refs FROM (
    SELECT 0 ordinal,item.raw_sha256 raw_sha256,item.raw_size raw_size,item.storage_key storage_key
    UNION ALL SELECT ordinal,raw_sha256,raw_size,storage_key FROM openlegal.corpus_capture_blob
      WHERE capture_id=item.id) b;
  IF jsonb_array_length(refs)>65 OR EXISTS(SELECT 1 FROM jsonb_array_elements(refs) ref
   WHERE (ref->>'ordinal')::integer NOT BETWEEN 0 AND 64
    OR ref->>'sha256'<>substring(ref->>'storage_key' from 4 for 64)
    OR substring(ref->>'storage_key' from 1 for 2)<>substring(ref->>'sha256' from 1 for 2)
    OR ref->>'storage_key' !~ '^[0-9a-f]{2}/[0-9a-f]{64}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR (SELECT count(DISTINCT ref->>'ordinal') FROM jsonb_array_elements(refs) ref)
      <>jsonb_array_length(refs) THEN
   RAISE EXCEPTION 'publication reference bound' USING ERRCODE='54000'; END IF;
  rows:=rows||jsonb_build_array(jsonb_build_object(
   'capture_id',item.id,'object_key',item.object_key,'object',jsonb_build_object(
    'jurisdiction',item.identity->'jurisdiction','provider',item.identity->'provider',
    'dataset',item.identity->'dataset','id',item.identity->'id'),
   'revision_id_sha256',encode(sha256(convert_to(item.revision_id,'UTF8')),'hex'),'event_sequence',item.event_sequence::text,
   'captured_at',item.captured_at::text,
   'body_sha256',encode(sha256(convert_to(item.payload#>>'{record,body}','UTF8')),'hex'),
   'body_bytes',octet_length(item.payload#>>'{record,body}')::text,
   'payload_sha256',encode(item.payload_sha256,'hex'),
   'resources_sha256',encode(sha256(convert_to(item.resource_text,'UTF8')),'hex'),
   'primary_rights',jsonb_build_object(
    'commercial_use',primary_resource#>'{rights,commercial_use}',
    'derivatives',true,'redistribution',true,
    'basis_sha256',encode(sha256(convert_to(primary_resource#>>'{rights,basis}','UTF8')),'hex'),
    'attribution_sha256',encode(sha256(convert_to(primary_resource#>>'{rights,attribution}','UTF8')),'hex'),
    'evidence_url_sha256',encode(sha256(convert_to(primary_resource#>>'{rights,evidence_url}','UTF8')),'hex')),
   'is_head',COALESCE(item.head_capture=item.id,false),'pending',item.pending,'blob_refs',refs));
  last_event:=item.event_sequence;last_id:=item.id;
 END LOOP;
 result:=jsonb_build_object('schema_version',1,'kind','openlegal_publication_page','rows',rows,
  'next_after',CASE WHEN more THEN jsonb_build_object('event_sequence',last_event::text,
    'capture_id',last_id) ELSE NULL END,'has_more',more);
 IF octet_length(result::text)>262144 THEN
  RAISE EXCEPTION 'publication output bound' USING ERRCODE='54000'; END IF;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION openlegal_checkpoint.publication_page(bigint,text,integer) FROM PUBLIC;
