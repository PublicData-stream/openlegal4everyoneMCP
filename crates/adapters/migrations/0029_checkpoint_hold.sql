-- Daily maintenance owns only a clean hold it created. This schema is separate
-- from general provider recovery; migrations provision no LOGIN or secret.
CREATE SCHEMA openlegal_checkpoint;
REVOKE ALL ON SCHEMA openlegal_checkpoint FROM PUBLIC;
CREATE TABLE openlegal_checkpoint.operation (
 operation_id uuid PRIMARY KEY CHECK(operation_id<>'00000000-0000-0000-0000-000000000000'),
 intent_sha256 text NOT NULL CHECK(intent_sha256 ~ '^[0-9a-f]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 database_actor name NOT NULL DEFAULT session_user,
 owned boolean NOT NULL,
 original_generation uuid NOT NULL,
 installed_generation uuid NOT NULL,
 clean_state jsonb NOT NULL,
 hold_result jsonb NOT NULL,
 released_at timestamptz,
 release_result jsonb,
 CHECK((released_at IS NULL)=(release_result IS NULL))
);
REVOKE ALL ON TABLE openlegal_checkpoint.operation FROM PUBLIC;

CREATE FUNCTION openlegal_checkpoint.state() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object(
  'budget',to_jsonb(b),
  'control',to_jsonb(c)-'generation'-'recovery_hold'-'hold_started_at',
  'admissions',COALESCE((SELECT jsonb_agg(to_jsonb(a) ORDER BY a.slot)
    FROM openlegal.provider_request_admission a),'[]'::jsonb),
  'canary_control',(SELECT to_jsonb(k) FROM openlegal_admin.provider_canary_control k),
  'canaries',COALESCE((SELECT jsonb_agg(to_jsonb(k) ORDER BY k.id)
    FROM openlegal_admin.provider_canary k JOIN openlegal_admin.provider_canary_control cc
    ON cc.required_id=k.id WHERE cc.singleton),'[]'::jsonb))
 FROM openlegal.provider_request_budget b CROSS JOIN openlegal_admin.provider_control c
 WHERE b.singleton AND c.singleton
$$;

CREATE FUNCTION openlegal_checkpoint.hold(p_operation uuid,p_intent_sha256 text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c openlegal_admin.provider_control%ROWTYPE;
 b openlegal.provider_request_budget%ROWTYPE;
 old openlegal_checkpoint.operation%ROWTYPE;
 slot integer; installed uuid; clean jsonb; result jsonb; owned boolean;
BEGIN
 IF p_operation IS NULL OR p_operation='00000000-0000-0000-0000-000000000000'
 OR p_intent_sha256 IS NULL OR p_intent_sha256 !~ '^[0-9a-f]{64}$'
 THEN RAISE EXCEPTION 'invalid checkpoint intent' USING ERRCODE='22023'; END IF;
 -- Match provider_apply's established lock order. No network or filesystem work
 -- runs under these locks. Physical closure remains the operator's obligation.
 PERFORM singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'corpus control unavailable' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT b FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT c FROM openlegal_admin.provider_control WHERE singleton FOR UPDATE;
 FOR slot IN 0..16 LOOP
  IF NOT pg_try_advisory_xact_lock(1869376611,1818326864+slot) THEN
   RAISE EXCEPTION 'provider owner remains live' USING ERRCODE='55P03'; END IF;
 END LOOP;
 SELECT * INTO old FROM openlegal_checkpoint.operation WHERE operation_id=p_operation FOR UPDATE;
 IF FOUND THEN
  IF old.intent_sha256<>p_intent_sha256 THEN
   RAISE EXCEPTION 'checkpoint identity conflict' USING ERRCODE='40001'; END IF;
  IF old.released_at IS NOT NULL OR NOT c.recovery_hold
    OR c.generation<>old.installed_generation OR openlegal_checkpoint.state()<>old.clean_state THEN
   RAISE EXCEPTION 'checkpoint historical receipt requires readback' USING ERRCODE='40001'; END IF;
  RETURN old.hold_result;
 END IF;
 -- A pre-existing hold is preserved, including its uncertainty and owner ledger.
 -- It is recorded as unowned and can never be released by this checkpoint.
 owned:=NOT c.recovery_hold;
 IF owned AND (b.unresolved_response OR b.admission_owner IS NOT NULL
   OR EXISTS(SELECT 1 FROM openlegal.provider_request_admission)) THEN
  RAISE EXCEPTION 'checkpoint requires clean drained provider' USING ERRCODE='55000';
 END IF;
 clean:=openlegal_checkpoint.state();
 installed:=CASE WHEN owned THEN pg_catalog.uuidv7() ELSE c.generation END;
 IF owned THEN
  UPDATE openlegal_admin.provider_control SET recovery_hold=true,generation=installed,
    hold_started_at=floor(extract(epoch FROM clock_timestamp()))::bigint WHERE singleton;
 END IF;
 result:=jsonb_build_object('operation_id',p_operation,'intent_sha256',p_intent_sha256,
   'owned',owned,'original_generation',c.generation,'installed_generation',installed,
   'provider_recovery_hold',true,'released',false);
 INSERT INTO openlegal_checkpoint.operation(operation_id,intent_sha256,owned,
   original_generation,installed_generation,clean_state,hold_result)
 VALUES(p_operation,p_intent_sha256,owned,c.generation,installed,clean,result);
 RETURN result;
END $$;

CREATE FUNCTION openlegal_checkpoint.release(p_operation uuid,p_intent_sha256 text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c openlegal_admin.provider_control%ROWTYPE;
 b openlegal.provider_request_budget%ROWTYPE;
 old openlegal_checkpoint.operation%ROWTYPE;
 slot integer; result jsonb; released_generation uuid;
BEGIN
 IF p_operation IS NULL OR p_intent_sha256 IS NULL
 THEN RAISE EXCEPTION 'invalid checkpoint intent' USING ERRCODE='22023'; END IF;
 PERFORM singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'corpus control unavailable' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT b FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT c FROM openlegal_admin.provider_control WHERE singleton FOR UPDATE;
 FOR slot IN 0..16 LOOP
  IF NOT pg_try_advisory_xact_lock(1869376611,1818326864+slot) THEN
   RAISE EXCEPTION 'provider owner remains live' USING ERRCODE='55P03'; END IF;
 END LOOP;
 SELECT * INTO STRICT old FROM openlegal_checkpoint.operation WHERE operation_id=p_operation FOR UPDATE;
 IF old.intent_sha256<>p_intent_sha256 THEN
  RAISE EXCEPTION 'checkpoint identity conflict' USING ERRCODE='40001'; END IF;
 IF old.release_result IS NOT NULL THEN
  IF c.recovery_hold OR c.generation::text<>(old.release_result->>'released_generation')
    OR openlegal_checkpoint.state()<>old.clean_state THEN
   RAISE EXCEPTION 'checkpoint historical receipt requires readback' USING ERRCODE='40001'; END IF;
  RETURN old.release_result;
 END IF;
 IF NOT old.owned THEN
  RAISE EXCEPTION 'checkpoint does not own provider hold' USING ERRCODE='55000'; END IF;
 IF NOT c.recovery_hold OR c.generation<>old.installed_generation
 OR b.unresolved_response OR b.admission_owner IS NOT NULL
 OR EXISTS(SELECT 1 FROM openlegal.provider_request_admission)
 OR openlegal_checkpoint.state()<>old.clean_state THEN
  RAISE EXCEPTION 'checkpoint provider state changed' USING ERRCODE='40001'; END IF;
 -- Never clear suspension, Retry-After, charges, owner evidence or waiting work.
 released_generation:=pg_catalog.uuidv7();
 UPDATE openlegal_admin.provider_control SET recovery_hold=false,generation=released_generation,
   hold_started_at=NULL WHERE singleton;
 result:=old.hold_result||jsonb_build_object('released',true,'provider_recovery_hold',false,
   'released_generation',released_generation);
 UPDATE openlegal_checkpoint.operation SET released_at=clock_timestamp(),release_result=result
  WHERE operation_id=p_operation;
 RETURN result;
END $$;

CREATE FUNCTION openlegal_checkpoint.readback(p_operation uuid) RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('hold',hold_result,'release',release_result,
   'created_at',created_at,'released_at',released_at)
 FROM openlegal_checkpoint.operation WHERE operation_id=p_operation
$$;

-- A LOGIN observer receives EXECUTE only. Row locks and the minimum underlying
-- table/slot-function permissions belong to a separate NOLOGIN function owner.
-- Admission locks its budget before provider control; after these SHARE locks
-- the volatile SELECT obtains one fresh state/ownership snapshot.
CREATE FUNCTION openlegal_checkpoint.observe(p_operation uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 IF p_operation IS NULL OR p_operation='00000000-0000-0000-0000-000000000000'
 THEN RAISE EXCEPTION 'invalid checkpoint operation' USING ERRCODE='22023'; END IF;
 PERFORM singleton FROM openlegal.provider_request_budget WHERE singleton FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'provider budget unavailable' USING ERRCODE='55000'; END IF;
 PERFORM singleton FROM openlegal_admin.provider_control WHERE singleton FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'provider control unavailable' USING ERRCODE='55000'; END IF;
 SELECT jsonb_build_object(
  'state',openlegal_checkpoint.state(),
  'hold_control',(SELECT to_jsonb(c) FROM openlegal_admin.provider_control c WHERE c.singleton),
  'admission_ownership',jsonb_build_object('legacy_locked',openlegal_admin.slot_locked(0),
    'slots',COALESCE((SELECT jsonb_agg(jsonb_build_object('owner',a.owner,'slot',a.slot,
     'locked',openlegal_admin.slot_locked(a.slot)) ORDER BY a.slot)
     FROM openlegal.provider_request_admission a),'[]'::jsonb)),
  'checkpoint_owner',(SELECT o.operation_id::text FROM openlegal_checkpoint.operation o
    JOIN openlegal_admin.provider_control c ON c.singleton AND c.recovery_hold
     AND o.installed_generation=c.generation WHERE o.owned AND o.released_at IS NULL),
  'checkpoint_readback',openlegal_checkpoint.readback(p_operation)) INTO result;
 RETURN result;
END $$;

-- Hashes/counts and retained physical identities only: no legal body, query
-- text, connection string or secret is returned by the operator observers.
CREATE FUNCTION openlegal_checkpoint.retained_tables() RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE t record; proof jsonb; result jsonb:='{}'::jsonb; total integer:=0;
BEGIN
 FOR t IN SELECT schemaname,tablename FROM pg_catalog.pg_tables
  WHERE schemaname IN ('openlegal','openlegal_admin','openlegal_replication','openlegal_checkpoint')
   OR (schemaname='public' AND tablename='_sqlx_migrations')
  ORDER BY schemaname,tablename LOOP
  total:=total+1;
  IF total>256 THEN RAISE EXCEPTION 'operator table bound' USING ERRCODE='54000'; END IF;
  EXECUTE format('SELECT jsonb_build_object(''rows'',count(*),''sha256'',encode(sha256('
   'COALESCE(string_agg(h,'''' ORDER BY h COLLATE "C"),'''')::bytea),''hex'')) '
   'FROM (SELECT encode(sha256(row_to_json(t)::text::bytea),''hex'') h FROM %I.%I t) source',
   t.schemaname,t.tablename) INTO proof;
  result:=result||jsonb_build_object(t.schemaname||'.'||t.tablename,proof);
 END LOOP;
 IF total=0 THEN RAISE EXCEPTION 'operator tables unavailable' USING ERRCODE='55000'; END IF;
 RETURN result;
END $$;

CREATE FUNCTION openlegal_checkpoint.retained_references() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY role,storage_key),'[]'::jsonb) FROM (
  SELECT * FROM (
   SELECT 'cache'::text role,storage_key,encode(sha256,'hex') sha256,size_bytes size
    FROM openlegal.blob_object WHERE ready
   UNION ALL SELECT 'corpus',storage_key,encode(raw_sha256,'hex'),raw_size FROM openlegal.corpus_capture
   UNION ALL SELECT 'corpus',storage_key,encode(raw_sha256,'hex'),raw_size FROM openlegal.corpus_capture_blob
   UNION ALL SELECT 'corpus',storage_key,encode(raw_sha256,'hex'),raw_size
    FROM openlegal.corpus_source_observation WHERE storage_key IS NOT NULL
  ) refs ORDER BY role,storage_key LIMIT 200001
 ) r
$$;

CREATE FUNCTION openlegal_checkpoint.database_position() RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF pg_is_in_recovery() THEN
  RETURN jsonb_build_object('system_identifier',(SELECT system_identifier::text FROM pg_control_system()),
   'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),
   'postmaster_started_at',pg_postmaster_start_time()::text,
   'replay_lsn',pg_last_wal_replay_lsn()::text,'receive_lsn',pg_last_wal_receive_lsn()::text,
   'in_recovery',true);
 END IF;
 RETURN jsonb_build_object('system_identifier',(SELECT system_identifier::text FROM pg_control_system()),
  'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),
  -- wal_lsn is the primary durable cut, matching capture_prefix; raw insertion
  -- progress belongs to separate physical-recovery diagnostics.
  'postmaster_started_at',pg_postmaster_start_time()::text,'wal_lsn',pg_current_wal_flush_lsn()::text,
  'index_ack',(SELECT index_ack FROM openlegal.corpus_control),
  'watermark',(SELECT next_event-1 FROM openlegal.corpus_control),'in_recovery',false);
END $$;

-- The effective server directory, not environment/argv guesses. Only the
-- NOLOGIN owner needs pg_read_all_settings; callers receive this one path.
CREATE FUNCTION openlegal_checkpoint.database_layout() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('data_directory',current_setting('data_directory'))
$$;

CREATE FUNCTION openlegal_checkpoint.client_sessions() RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('sessions',COALESCE(jsonb_agg(jsonb_build_object(
  'pid',pid,'backend_start',backend_start,'state',state) ORDER BY pid),'[]'::jsonb))
 FROM pg_catalog.pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
 -- backend_type/state are masked for other roles without pg_read_all_stats.
 -- Filtering that nullable type could falsely certify zero client sessions.
 -- Include every visible PID in this database; background sessions may cause
 -- conservative refusal. No broad statistics or query-text grant is needed.
$$;

CREATE FUNCTION openlegal_checkpoint.storage_metadata() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object(
  'corpus_control',(SELECT to_jsonb(c) FROM openlegal.corpus_control c WHERE singleton),
  'cache_storage',(SELECT to_jsonb(c) FROM openlegal.cache_storage c WHERE singleton),
  'cache_totals',jsonb_build_object(
   'snapshots',(SELECT count(*)::bigint FROM openlegal.cache_snapshot),
   'queries',(SELECT count(*)::bigint FROM openlegal.cache_query),
   'referenced_bytes',COALESCE((SELECT sum(b.size_bytes)::bigint FROM openlegal.blob_object b
    WHERE EXISTS(SELECT 1 FROM openlegal.cache_snapshot s WHERE s.raw_blob_sha256=b.sha256)),0)))
$$;

CREATE FUNCTION openlegal_checkpoint.canary_observation(p_canary uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_canary IS NULL OR p_canary='00000000-0000-0000-0000-000000000000'
 THEN RAISE EXCEPTION 'invalid canary identity' USING ERRCODE='22023'; END IF;
 RETURN jsonb_build_object('schema_version',1,'kind','openlegal_canary_observation',
  'observed_at',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'required_id',(SELECT required_id FROM openlegal_admin.provider_canary_control WHERE singleton),
  'canary',(SELECT jsonb_build_object('id',id,'operation_id',operation_id,'consumed',consumed,
   'started_at',started_at,'deadline',deadline,'last_observed_at',last_observed_at,'ended_at',ended_at)
   FROM openlegal_admin.provider_canary WHERE id=p_canary));
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA openlegal_checkpoint FROM PUBLIC;
