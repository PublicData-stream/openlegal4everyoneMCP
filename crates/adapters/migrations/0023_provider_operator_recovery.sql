-- Protected operator recovery state. DBA-owned roles receive named EXECUTE
-- grants separately; the runtime's broad openlegal table grants do not apply.
CREATE SCHEMA openlegal_admin;
REVOKE ALL ON SCHEMA openlegal_admin FROM PUBLIC;
CREATE TABLE openlegal_admin.provider_control (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 recovery_hold boolean NOT NULL DEFAULT false,
 generation uuid NOT NULL DEFAULT pg_catalog.uuidv7(),
 hold_started_at bigint,
 legacy_identity uuid,
 operator_suspended_at bigint
);
INSERT INTO openlegal_admin.provider_control(singleton,legacy_identity)
 SELECT true,CASE WHEN unresolved_response THEN pg_catalog.uuidv7() END
 FROM openlegal.provider_request_budget WHERE singleton;
CREATE TABLE openlegal_admin.uncertainty_observation (
 identity uuid PRIMARY KEY,
 kind text NOT NULL CHECK(kind IN ('legacy','slot')),
 owner uuid,
 slot integer CHECK(slot BETWEEN 1 AND 16),
 first_observed_at bigint,
 CHECK((kind='legacy' AND slot IS NULL) OR (kind='slot' AND owner IS NOT NULL AND slot IS NOT NULL))
);
-- A migration cannot reconstruct when legacy uncertainty was first observed.
INSERT INTO openlegal_admin.uncertainty_observation(identity,kind,owner)
 SELECT c.legacy_identity,'legacy',b.admission_owner FROM openlegal_admin.provider_control c
 CROSS JOIN openlegal.provider_request_budget b WHERE c.singleton AND b.singleton AND c.legacy_identity IS NOT NULL;
CREATE TABLE openlegal_admin.provider_recovery_audit (
 operation_id uuid PRIMARY KEY,
 plan jsonb NOT NULL,
 actor text NOT NULL,
 database_actor text NOT NULL,
 actual_writers_stopped_at bigint NOT NULL,
 applied_at bigint NOT NULL,
 before_state jsonb NOT NULL,
 result jsonb NOT NULL
);
REVOKE ALL ON ALL TABLES IN SCHEMA openlegal_admin FROM PUBLIC;

ALTER TABLE openlegal.collection_request ADD COLUMN provider_deferral_fingerprint jsonb
 CHECK(provider_deferral_fingerprint IS NULL OR (jsonb_typeof(provider_deferral_fingerprint)='object' AND pg_column_size(provider_deferral_fingerprint)<=8192));
ALTER TABLE openlegal.corpus_job ADD COLUMN provider_deferral_fingerprint jsonb
 CHECK(provider_deferral_fingerprint IS NULL OR (jsonb_typeof(provider_deferral_fingerprint)='object' AND pg_column_size(provider_deferral_fingerprint)<=8192));

CREATE FUNCTION openlegal_admin.slot_locked(p_slot integer) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='advisory'
 AND database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
 AND pid<>pg_catalog.pg_backend_pid() AND classid=1869376611::oid AND objid=(1818326864+p_slot)::oid AND objsubid=2 AND granted)
$$;

CREATE FUNCTION openlegal_admin.provider_state() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('storage_id',(SELECT id FROM openlegal.cache_storage WHERE singleton),
 'corpus_control',(SELECT to_jsonb(cc) FROM openlegal.corpus_control cc WHERE singleton),
 'schema',(SELECT jsonb_agg(jsonb_build_object('version',version,'checksum',encode(checksum,'hex'),'success',success) ORDER BY version) FROM public._sqlx_migrations),
 'budget',to_jsonb(b),'control',to_jsonb(c),
 'legacy_lock_held',openlegal_admin.slot_locked(0),
 'ledger',COALESCE((SELECT jsonb_agg(to_jsonb(a)||jsonb_build_object('lock_held',openlegal_admin.slot_locked(a.slot)) ORDER BY a.slot)
 FROM openlegal.provider_request_admission a),'[]'::jsonb))
 FROM openlegal.provider_request_budget b CROSS JOIN openlegal_admin.provider_control c
 WHERE b.singleton AND c.singleton
$$;

CREATE FUNCTION openlegal_admin.wait_state(p_kind text,p_id uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 IF p_kind='collection_request' THEN
 SELECT jsonb_build_object('status',status,'launched_at',launched_at,'lease_until',lease_until,
 'reason',reason,'expires_at',expires_at,'operation_attempt_limit',operation_attempt_limit,
 'operation_timeout_secs',operation_timeout_secs,'provider_deferral_fingerprint',provider_deferral_fingerprint) INTO result
 FROM openlegal.collection_request WHERE id=p_id;
 ELSIF p_kind='detail_job' THEN
 SELECT jsonb_build_object('status',j.status,'expected_version',j.expected_version,'attempts',j.attempts,
 'lease_until',j.lease_until,'error_category',j.error_category,'explicit_request_id',j.explicit_request_id,
 'explicit_recovery_at',j.explicit_recovery_at,'object_version',o.version,'withdrawn',o.withdrawn,
 'pending',o.pending,'install_head',j.install_head,'revision_id',j.revision_id,'desired_head_revision',o.desired_head_revision,
 'origin',j.source_metadata->>'collection_origin','created_at',j.created_at,'provider_deferral_fingerprint',j.provider_deferral_fingerprint)
 INTO result FROM openlegal.corpus_job j JOIN openlegal.corpus_object o USING(object_key) WHERE j.id=p_id;
 ELSE RAISE EXCEPTION 'invalid wait kind' USING ERRCODE='22023'; END IF;
 RETURN result;
END $$;

CREATE FUNCTION openlegal_admin.provider_inspect() RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE waits jsonb; total bigint;
BEGIN
 SELECT count(*) INTO total FROM (
 SELECT id FROM openlegal.collection_request WHERE status='deferred'
 UNION ALL SELECT id FROM openlegal.corpus_job WHERE status='running' AND error_category='budget_wait') q;
 SELECT COALESCE(jsonb_agg(jsonb_build_object('kind',kind,'id',id,'state',openlegal_admin.wait_state(kind,id)) ORDER BY kind,id),'[]'::jsonb)
 INTO waits FROM (
 SELECT * FROM (SELECT 'collection_request'::text kind,id FROM openlegal.collection_request WHERE status='deferred'
 UNION ALL SELECT 'detail_job',id FROM openlegal.corpus_job WHERE status='running' AND error_category='budget_wait') all_waits
 ORDER BY kind,id LIMIT 256) q;
 RETURN jsonb_build_object('version',1,'observed_at',floor(extract(epoch FROM clock_timestamp()))::bigint,
 'state',openlegal_admin.provider_state(),'waiting',waits,'waiting_truncated',total>256);
END $$;

CREATE FUNCTION openlegal_admin.provider_inspect_selected(p_selected jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE item jsonb; waits jsonb:='[]'; state jsonb;
BEGIN
 IF jsonb_typeof(p_selected) IS DISTINCT FROM 'array' OR jsonb_array_length(p_selected)>256
 THEN RAISE EXCEPTION 'invalid selected wait list' USING ERRCODE='22023'; END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(p_selected) LOOP
  IF jsonb_typeof(item) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'invalid selected wait' USING ERRCODE='22023'; END IF;
  IF NOT item ?& ARRAY['kind','id'] OR EXISTS(SELECT 1 FROM jsonb_object_keys(item) k WHERE k NOT IN ('kind','id'))
  OR jsonb_typeof(item->'id') IS DISTINCT FROM 'string' OR jsonb_typeof(item->'kind') IS DISTINCT FROM 'string'
  OR item->>'kind' NOT IN ('collection_request','detail_job') OR octet_length(item->>'id')<>36
  THEN RAISE EXCEPTION 'invalid selected wait' USING ERRCODE='22023'; END IF;
  state:=openlegal_admin.wait_state(item->>'kind',(item->>'id')::uuid);
  IF state IS NULL THEN RAISE EXCEPTION 'selected wait was not found' USING ERRCODE='22023'; END IF;
  waits:=waits||jsonb_build_array(item||jsonb_build_object('state',state));
 END LOOP;
 IF (SELECT count(*) FROM jsonb_array_elements(p_selected))<>(SELECT count(DISTINCT (value->>'kind',value->>'id')) FROM jsonb_array_elements(p_selected))
 THEN RAISE EXCEPTION 'duplicate selected wait' USING ERRCODE='22023'; END IF;
 RETURN jsonb_build_object('version',1,'observed_at',floor(extract(epoch FROM clock_timestamp()))::bigint,
 'state',openlegal_admin.provider_state(),'waiting',waits,'waiting_truncated',false);
END $$;

CREATE FUNCTION openlegal_admin.provider_held() RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT recovery_hold FROM openlegal_admin.provider_control WHERE singleton
$$;

-- Collector-only instrumentation. Call inside the admission transaction, then
-- COMMIT the observation before returning a refusal. Public reads never call it.
CREATE FUNCTION openlegal_admin.observe_uncertainty() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE b openlegal.provider_request_budget%ROWTYPE; c openlegal_admin.provider_control%ROWTYPE; a record; stamp bigint;
BEGIN
 SELECT * INTO STRICT b FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT c FROM openlegal_admin.provider_control WHERE singleton FOR UPDATE;
 stamp:=floor(extract(epoch FROM clock_timestamp()))::bigint;
 IF b.unresolved_response THEN
  IF c.legacy_identity IS NULL THEN
   UPDATE openlegal_admin.provider_control SET legacy_identity=pg_catalog.uuidv7() WHERE singleton RETURNING * INTO c;
  END IF;
  INSERT INTO openlegal_admin.uncertainty_observation(identity,kind,owner,first_observed_at)
  VALUES(c.legacy_identity,'legacy',b.admission_owner,stamp)
  ON CONFLICT(identity) DO UPDATE SET first_observed_at=EXCLUDED.first_observed_at
  WHERE openlegal_admin.uncertainty_observation.first_observed_at IS NULL;
 END IF;
 FOR a IN SELECT * FROM openlegal.provider_request_admission LOOP
  IF NOT openlegal_admin.slot_locked(a.slot) THEN
   INSERT INTO openlegal_admin.uncertainty_observation(identity,kind,owner,slot,first_observed_at)
   VALUES(a.owner,'slot',a.owner,a.slot,stamp) ON CONFLICT(identity) DO NOTHING;
  END IF;
 END LOOP;
END $$;

CREATE FUNCTION openlegal_admin.provider_blocker_fingerprint() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('legacy_identity',CASE WHEN b.unresolved_response THEN c.legacy_identity END,
 'hold_generation',CASE WHEN c.recovery_hold THEN c.generation END,
 'slot_owners',COALESCE((SELECT jsonb_agg(owner::text ORDER BY owner::text) FROM openlegal.provider_request_admission WHERE NOT openlegal_admin.slot_locked(slot)),'[]'::jsonb))
 FROM openlegal.provider_request_budget b CROSS JOIN openlegal_admin.provider_control c WHERE b.singleton AND c.singleton
$$;

CREATE FUNCTION openlegal_admin.provider_diagnostic() RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE b openlegal.provider_request_budget%ROWTYPE; c openlegal_admin.provider_control%ROWTYPE;
 stamp bigint; active integer; abandoned integer; first_seen bigint; modes jsonb:='{}'; mode text;
 used bigint; quota integer; reason text; recheck bigint; review boolean; ready boolean; spacing bigint; locked_ledger jsonb;
BEGIN
 -- Read-only row ownership serializes against normal response settlement.
 -- Fresh SPI reads after acquiring this lock cannot retain a deleted ledger
 -- row while observing its subsequently released session lock.
 SELECT * INTO STRICT b FROM openlegal.provider_request_budget WHERE singleton FOR SHARE;
 SELECT * INTO STRICT c FROM openlegal_admin.provider_control WHERE singleton;
 stamp:=floor(extract(epoch FROM clock_timestamp()))::bigint;
 -- Capture each slot lock once. A crashed connection can still disappear
 -- during observation; this is an admission hint, never a reservation promise.
 SELECT COALESCE(jsonb_agg(jsonb_build_object('owner',owner,'lock_held',openlegal_admin.slot_locked(slot))),'[]')
 INTO locked_ledger FROM openlegal.provider_request_admission;
 SELECT count(*) FILTER(WHERE (a->>'lock_held')::boolean),count(*) FILTER(WHERE NOT (a->>'lock_held')::boolean)
 INTO active,abandoned FROM jsonb_array_elements(locked_ledger) a;
 SELECT CASE WHEN count(*) FILTER(WHERE o.first_observed_at IS NULL)>0 THEN NULL ELSE min(o.first_observed_at) END
 INTO first_seen FROM (
 SELECT c.legacy_identity identity WHERE b.unresolved_response
 UNION ALL SELECT (a->>'owner')::uuid FROM jsonb_array_elements(locked_ledger) a WHERE NOT (a->>'lock_held')::boolean
 ) current_uncertainty LEFT JOIN openlegal_admin.uncertainty_observation o USING(identity);
 spacing:=b.next_request_at_ms/1000+CASE WHEN b.next_request_at_ms%1000<>0 THEN 1 ELSE 0 END;
 FOREACH mode IN ARRAY ARRAY['continuous','on_demand'] LOOP
  used:=CASE WHEN b.utc_day=stamp/86400 THEN CASE WHEN mode='continuous' THEN b.daily_used ELSE b.on_demand_used END ELSE 0 END;
  quota:=CASE WHEN mode='continuous' THEN b.continuous_daily_limit ELSE b.on_demand_daily_limit END;
  review:=false; ready:=false; recheck:=GREATEST(stamp+10,b.next_allowed_at,spacing);
  IF c.recovery_hold THEN reason:='provider_recovery_hold'; review:=true; recheck:=stamp+3600;
  ELSIF b.unresolved_response OR abandoned>0 THEN reason:='provider_response_uncertain'; review:=true; recheck:=stamp+3600;
  ELSIF b.operator_suspended THEN reason:='provider_suspended'; review:=true; recheck:=stamp+3600;
  ELSIF quota IS NOT NULL AND used>=quota THEN reason:='provider_daily_limit'; recheck:=GREATEST(recheck,(stamp/86400+1)*86400+10);
  ELSIF b.next_allowed_at>stamp THEN reason:='provider_retry_after';
  ELSIF active>=b.max_in_flight THEN reason:='provider_admission_wait'; recheck:=stamp+5;
  ELSIF spacing>stamp THEN reason:='provider_admission_wait';
  ELSE reason:='ready'; ready:=true; recheck:=NULL; END IF;
  modes:=modes||jsonb_build_object(mode,jsonb_build_object('reason',reason,'ready',ready,'recheck_at',recheck,'requires_operator_review',review));
 END LOOP;
 RETURN jsonb_build_object('version',1,'observed_at',stamp,'recovery_hold',c.recovery_hold,'hold_generation',c.generation,
 'hold_started_at',c.hold_started_at,'operator_suspended',b.operator_suspended,'operator_suspended_at',c.operator_suspended_at,
 'legacy_uncertain',b.unresolved_response,'abandoned_slots',abandoned,'active_slots',active,'first_observed_at',first_seen,
 'blocked_since',CASE WHEN NOT b.unresolved_response AND abandoned=0 AND NOT b.operator_suspended AND c.recovery_hold THEN c.hold_started_at ELSE NULL END)||modes;
END $$;

CREATE FUNCTION openlegal_admin.provider_readback(p_operation uuid) RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT result FROM openlegal_admin.provider_recovery_audit WHERE operation_id=p_operation
$$;

CREATE FUNCTION openlegal_admin.required_object(p_value jsonb,p_keys text[]) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF jsonb_typeof(p_value) IS DISTINCT FROM 'object' THEN RETURN false; END IF;
 RETURN p_value ?& p_keys AND NOT EXISTS(SELECT 1 FROM jsonb_object_keys(p_value) k WHERE NOT k=ANY(p_keys));
END $$;
CREATE FUNCTION openlegal_admin.valid_text(p_value jsonb,p_max integer) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE val text;
BEGIN
 IF jsonb_typeof(p_value) IS DISTINCT FROM 'string' THEN RETURN false; END IF;
 val:=p_value#>>'{}';
 RETURN octet_length(val) BETWEEN 1 AND p_max AND btrim(val)=val AND val !~ '[[:cntrl:]]' AND val !~ '^[[:space:]]|[[:space:]]$';
END $$;
CREATE FUNCTION openlegal_admin.valid_epoch(p_value jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE val text;
BEGIN
 IF jsonb_typeof(p_value) IS DISTINCT FROM 'number' THEN RETURN false; END IF;
 val:=p_value#>>'{}';
 RETURN val ~ '^[0-9]{1,18}$';
END $$;

CREATE FUNCTION openlegal_admin.provider_apply(p_plan jsonb) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE request jsonb; before_state jsonb; old_plan jsonb; prior jsonb; result jsonb; stamp bigint;
 operation uuid; stop_at bigint; slot integer; target_owner text; wait jsonb; expected jsonb; current_wait jsonb;
 item jsonb; request_id uuid; job_id uuid; resume_at bigint; daily bigint; budget openlegal.provider_request_budget%ROWTYPE;
 control openlegal_admin.provider_control%ROWTYPE; new_generation uuid; advanced jsonb:='[]'; removed jsonb:='[]';
 resume boolean; resolve_legacy boolean; unchanged jsonb:='[]';
BEGIN
 IF octet_length(p_plan::text)>262144 OR p_plan->>'version' IS DISTINCT FROM '1' OR p_plan->'snapshot'->>'version' IS DISTINCT FROM '1'
 OR jsonb_typeof(p_plan->'created_at') IS DISTINCT FROM 'number' OR jsonb_typeof(p_plan->'expires_at') IS DISTINCT FROM 'number'
 OR jsonb_typeof(p_plan->'snapshot'->'observed_at') IS DISTINCT FROM 'number'
 OR jsonb_typeof(p_plan->'snapshot'->'state') IS DISTINCT FROM 'object'
 OR jsonb_typeof(p_plan->'snapshot'->'waiting') IS DISTINCT FROM 'array'
 OR jsonb_array_length(p_plan->'snapshot'->'waiting')>256
 OR jsonb_typeof(p_plan->'request') IS DISTINCT FROM 'object' THEN
  RAISE EXCEPTION 'invalid recovery plan' USING ERRCODE='22023'; END IF;
 IF NOT openlegal_admin.required_object(p_plan,ARRAY['version','operation_id','created_at','expires_at','snapshot','request'])
 OR NOT openlegal_admin.required_object(p_plan->'snapshot',ARRAY['version','observed_at','state','waiting','waiting_truncated'])
 OR NOT openlegal_admin.required_object(p_plan->'request',ARRAY['actor','reason','quiescence','resolve_legacy','owners','resume','waits'])
 OR NOT openlegal_admin.required_object(p_plan->'request'->'quiescence',ARRAY['writers_stopped_at','deployment_revision','stopped_writer_ids','evidence_reference'])
 OR NOT openlegal_admin.valid_text(p_plan->'operation_id',36)
 OR NOT openlegal_admin.valid_epoch(p_plan->'created_at') OR NOT openlegal_admin.valid_epoch(p_plan->'expires_at')
 OR NOT openlegal_admin.valid_epoch(p_plan->'snapshot'->'observed_at')
 OR NOT openlegal_admin.valid_epoch(p_plan->'request'->'quiescence'->'writers_stopped_at')
 OR NOT openlegal_admin.valid_text(p_plan->'request'->'actor',200)
 OR NOT openlegal_admin.valid_text(p_plan->'request'->'reason',1000)
 OR NOT openlegal_admin.valid_text(p_plan->'request'->'quiescence'->'deployment_revision',200)
 OR NOT openlegal_admin.valid_text(p_plan->'request'->'quiescence'->'evidence_reference',1000)
 OR jsonb_typeof(p_plan->'snapshot'->'waiting_truncated') IS DISTINCT FROM 'boolean'
 THEN RAISE EXCEPTION 'invalid required recovery fields' USING ERRCODE='22023'; END IF;
 request:=p_plan->'request'; operation:=(p_plan->>'operation_id')::uuid;
 IF EXISTS(SELECT 1 FROM jsonb_object_keys(p_plan) k WHERE k NOT IN ('version','operation_id','created_at','expires_at','snapshot','request'))
 OR EXISTS(SELECT 1 FROM jsonb_object_keys(request) k WHERE k NOT IN ('actor','reason','quiescence','resolve_legacy','owners','resume','waits'))
 THEN RAISE EXCEPTION 'unknown recovery field' USING ERRCODE='22023'; END IF;
 SELECT audit.plan,audit.result INTO old_plan,prior FROM openlegal_admin.provider_recovery_audit audit WHERE audit.operation_id=operation;
 IF FOUND THEN
  IF old_plan<>p_plan THEN RAISE EXCEPTION 'operation identity conflict' USING ERRCODE='40001'; END IF;
  RETURN prior;
 END IF;
 IF jsonb_typeof(request->'resume') IS DISTINCT FROM 'boolean'
 OR jsonb_typeof(request->'resolve_legacy') IS DISTINCT FROM 'boolean'
 OR jsonb_typeof(request->'owners') IS DISTINCT FROM 'array'
 OR jsonb_typeof(request->'waits') IS DISTINCT FROM 'array'
 OR jsonb_array_length(request->'owners')>16 OR jsonb_array_length(request->'waits')>256
 OR COALESCE(length(request->>'actor'),0) NOT BETWEEN 1 AND 200 OR COALESCE(length(request->>'reason'),0) NOT BETWEEN 1 AND 1000
 OR request->>'actor' ~ '[[:cntrl:]]' OR request->>'reason' ~ '[[:cntrl:]]'
 OR jsonb_typeof(request->'quiescence'->'writers_stopped_at') IS DISTINCT FROM 'number'
 OR jsonb_typeof(request->'quiescence'->'stopped_writer_ids') IS DISTINCT FROM 'array'
 OR jsonb_array_length(request->'quiescence'->'stopped_writer_ids') NOT BETWEEN 1 AND 64
 OR COALESCE(length(request->'quiescence'->>'deployment_revision'),0) NOT BETWEEN 1 AND 200
 OR COALESCE(length(request->'quiescence'->>'evidence_reference'),0) NOT BETWEEN 1 AND 1000
 THEN RAISE EXCEPTION 'invalid recovery request' USING ERRCODE='22023'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(request->'quiescence'->'stopped_writer_ids') i WHERE NOT openlegal_admin.valid_text(i,200))
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(request->'owners') i WHERE NOT openlegal_admin.valid_text(i,36))
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(request->'waits') i WHERE
 NOT openlegal_admin.required_object(i,ARRAY['kind','id','review_reason']) OR NOT openlegal_admin.valid_text(i->'id',36)
 OR NOT openlegal_admin.valid_text(i->'review_reason',1000) OR i->>'kind' NOT IN ('collection_request','detail_job'))
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(p_plan->'snapshot'->'waiting') i WHERE
 NOT openlegal_admin.required_object(i,ARRAY['kind','id','state']) OR NOT openlegal_admin.valid_text(i->'id',36)
 OR jsonb_typeof(i->'state') IS DISTINCT FROM 'object' OR i->>'kind' NOT IN ('collection_request','detail_job'))
 THEN RAISE EXCEPTION 'invalid recovery review fields' USING ERRCODE='22023'; END IF;
 resume:=(request->>'resume')::boolean; resolve_legacy:=(request->>'resolve_legacy')::boolean;
 IF NOT resume AND jsonb_array_length(request->'waits')<>0 THEN
 RAISE EXCEPTION 'wait reviews require resume intent' USING ERRCODE='22023'; END IF;
 stamp:=floor(extract(epoch FROM clock_timestamp()))::bigint;
 stop_at:=(request->'quiescence'->>'writers_stopped_at')::bigint;
 IF (p_plan->>'created_at')::bigint>stamp OR (p_plan->>'expires_at')::bigint<=stamp
 OR (p_plan->>'expires_at')::bigint<>(p_plan->>'created_at')::bigint+900
 OR (p_plan->>'created_at')::bigint<>(p_plan->'snapshot'->>'observed_at')::bigint
 OR stop_at<0 OR stop_at>(p_plan->>'created_at')::bigint THEN
 RAISE EXCEPTION 'expired or invalid recovery timing' USING ERRCODE='22023'; END IF;
 -- Lock order matches claim and policy changes; no upstream/blob operation occurs.
 PERFORM singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT budget FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT control FROM openlegal_admin.provider_control WHERE singleton FOR UPDATE;
 -- Serialize repeated operation IDs before state comparison.
 SELECT audit.plan,audit.result INTO old_plan,prior FROM openlegal_admin.provider_recovery_audit audit WHERE audit.operation_id=operation;
 IF FOUND THEN
  IF old_plan<>p_plan THEN RAISE EXCEPTION 'operation identity conflict' USING ERRCODE='40001'; END IF;
  RETURN prior;
 END IF;
 FOR slot IN 0..16 LOOP
  IF NOT pg_try_advisory_xact_lock(1869376611,1818326864+slot) THEN
   RAISE EXCEPTION 'provider owner remains live' USING ERRCODE='55P03'; END IF;
 END LOOP;
 before_state:=openlegal_admin.provider_state();
 IF before_state IS DISTINCT FROM p_plan->'snapshot'->'state' THEN
  RAISE EXCEPTION 'provider snapshot changed' USING ERRCODE='40001'; END IF;
 IF EXISTS(SELECT 1 FROM openlegal.provider_request_admission WHERE started_at_ms/1000>stop_at) THEN
 RAISE EXCEPTION 'writer evidence predates request start' USING ERRCODE='40001'; END IF;
 IF resolve_legacy IS DISTINCT FROM budget.unresolved_response THEN
 RAISE EXCEPTION 'legacy target mismatch' USING ERRCODE='40001'; END IF;
 -- Offline recovery resolves the exact entire ledger; partial forced owner
 -- cleanup is deliberately unsupported.
 IF (SELECT COALESCE(jsonb_agg(owner::text ORDER BY owner::text),'[]') FROM openlegal.provider_request_admission)
 IS DISTINCT FROM (SELECT COALESCE(jsonb_agg(value ORDER BY value),'[]') FROM jsonb_array_elements_text(request->'owners')) THEN
 RAISE EXCEPTION 'owner target mismatch' USING ERRCODE='40001'; END IF;
 IF NOT budget.unresolved_response AND jsonb_array_length(request->'owners')=0 AND NOT control.recovery_hold THEN
 RAISE EXCEPTION 'no recoverable evidence or hold' USING ERRCODE='22023'; END IF;
 -- Validate every selected lease before any mutation. Unknown causes require
 -- an explicit per-ID review; they are never inferred/backfilled.
 IF (SELECT count(*) FROM jsonb_array_elements(request->'waits')) <>
 (SELECT count(DISTINCT (value->>'kind',value->>'id')) FROM jsonb_array_elements(request->'waits')) THEN
 RAISE EXCEPTION 'duplicate wait review' USING ERRCODE='22023'; END IF;
 FOR wait IN SELECT value FROM jsonb_array_elements(request->'waits') LOOP
  IF COALESCE(length(wait->>'review_reason'),0) NOT BETWEEN 1 AND 1000 OR wait->>'review_reason' ~ '[[:cntrl:]]' THEN
   RAISE EXCEPTION 'missing wait review' USING ERRCODE='22023'; END IF;
  request_id:=(wait->>'id')::uuid;
  IF wait->>'kind'='collection_request' THEN
   PERFORM id FROM openlegal.collection_request WHERE id=request_id FOR UPDATE;
  ELSIF wait->>'kind'='detail_job' THEN
   PERFORM j.id FROM openlegal.corpus_job j JOIN openlegal.corpus_object o USING(object_key) WHERE j.id=request_id FOR UPDATE OF j,o;
  ELSE RAISE EXCEPTION 'invalid wait kind' USING ERRCODE='22023'; END IF;
  SELECT value->'state' INTO expected FROM jsonb_array_elements(p_plan->'snapshot'->'waiting')
   WHERE value->>'kind'=wait->>'kind' AND value->>'id'=wait->>'id';
  current_wait:=openlegal_admin.wait_state(wait->>'kind',request_id);
  IF expected IS NULL OR current_wait IS DISTINCT FROM expected THEN
   RAISE EXCEPTION 'wait snapshot changed' USING ERRCODE='40001'; END IF;
  IF wait->>'kind'='collection_request' THEN
   IF current_wait->>'status'<>'deferred' OR (current_wait->>'expires_at')::bigint<=stamp THEN
    RAISE EXCEPTION 'wait is not deferred' USING ERRCODE='40001'; END IF;
  ELSE
   IF current_wait->>'status'<>'running' OR current_wait->>'error_category'<>'budget_wait'
   OR (current_wait->>'withdrawn')::boolean OR NOT (current_wait->>'pending')::boolean
   OR (current_wait->>'object_version')::bigint<>(current_wait->>'expected_version')::bigint
   OR (current_wait->>'attempts')::integer>=budget.max_job_attempts
   OR ((current_wait->>'install_head')::boolean AND current_wait->>'revision_id' IS DISTINCT FROM current_wait->>'desired_head_revision')
   OR (current_wait->>'explicit_request_id' IS NOT NULL)
   OR COALESCE(current_wait->>'origin','')='explicit'
   THEN RAISE EXCEPTION 'detail ownership prevents resume' USING ERRCODE='40001'; END IF;
   IF EXISTS(SELECT 1 FROM openlegal.corpus_job active JOIN openlegal.corpus_job selected USING(object_key)
    WHERE selected.id=request_id AND active.id<>selected.id AND active.status='running' AND active.lease_until>stamp) THEN
    RAISE EXCEPTION 'another detail claim remains active' USING ERRCODE='40001'; END IF;
  END IF;
 END LOOP;
 -- Lock waits can be arbitrarily long for a direct SQL caller. Re-evaluate
 -- the server clock immediately before mutation, not before lock acquisition.
 stamp:=floor(extract(epoch FROM clock_timestamp()))::bigint;
 IF (p_plan->>'created_at')::bigint>stamp OR (p_plan->>'expires_at')::bigint<=stamp THEN
 RAISE EXCEPTION 'recovery plan expired while acquiring locks' USING ERRCODE='22023'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(request->'waits') w
 JOIN openlegal.collection_request r ON r.id=(w->>'id')::uuid
 WHERE w->>'kind'='collection_request' AND r.expires_at<=stamp) THEN
 RAISE EXCEPTION 'selected request expired while acquiring locks' USING ERRCODE='40001'; END IF;
 -- Retain identity/first-observation history before deleting mutable fences.
 INSERT INTO openlegal_admin.uncertainty_observation(identity,kind,owner,slot)
 SELECT a.owner,'slot',a.owner,a.slot FROM openlegal.provider_request_admission a ON CONFLICT(identity) DO NOTHING;
 IF resolve_legacy AND control.legacy_identity IS NULL THEN
 RAISE EXCEPTION 'legacy identity must be established before planning' USING ERRCODE='40001'; END IF;
 IF resolve_legacy THEN
  UPDATE openlegal.provider_request_budget SET unresolved_response=false,admission_owner=NULL WHERE singleton;
  UPDATE openlegal_admin.provider_control SET legacy_identity=NULL WHERE singleton;
 END IF;
 FOR target_owner IN SELECT value FROM jsonb_array_elements_text(request->'owners') LOOP
  DELETE FROM openlegal.provider_request_admission WHERE provider_request_admission.owner=target_owner::uuid;
  removed:=removed||jsonb_build_array(target_owner);
 END LOOP;
 new_generation:=pg_catalog.uuidv7();
 UPDATE openlegal_admin.provider_control SET recovery_hold=NOT resume,generation=new_generation,
 hold_started_at=CASE WHEN resume THEN NULL ELSE COALESCE(hold_started_at,stamp) END WHERE singleton;
 IF resume THEN
  FOR wait IN SELECT value FROM jsonb_array_elements(request->'waits') LOOP
   request_id:=(wait->>'id')::uuid;
   resume_at:=GREATEST(stamp+1,budget.next_allowed_at,budget.next_request_at_ms/1000+CASE WHEN budget.next_request_at_ms%1000<>0 THEN 1 ELSE 0 END);
   -- Suspension remains authoritative. Do not advance selected leases while it
   -- persists; global resume only releases the separate recovery hold.
   IF budget.operator_suspended THEN
    unchanged:=unchanged||jsonb_build_array(jsonb_build_object('kind',wait->>'kind','id',wait->>'id','reason','provider_suspended'));
    CONTINUE; END IF;
   IF wait->>'kind'='collection_request' THEN
    daily:=CASE WHEN budget.utc_day=stamp/86400 THEN budget.on_demand_used ELSE 0 END;
    IF budget.on_demand_daily_limit IS NOT NULL AND daily>=budget.on_demand_daily_limit THEN
     unchanged:=unchanged||jsonb_build_array(jsonb_build_object('kind',wait->>'kind','id',wait->>'id','reason','provider_daily_limit'));
     CONTINUE; END IF;
    UPDATE openlegal.collection_request SET lease_until=resume_at WHERE id=request_id AND lease_until>resume_at;
   ELSE
    daily:=CASE WHEN budget.utc_day=stamp/86400 THEN budget.daily_used ELSE 0 END;
    IF budget.continuous_daily_limit IS NOT NULL AND daily>=budget.continuous_daily_limit THEN
     unchanged:=unchanged||jsonb_build_array(jsonb_build_object('kind',wait->>'kind','id',wait->>'id','reason','provider_daily_limit'));
     CONTINUE; END IF;
    UPDATE openlegal.corpus_job SET lease_until=resume_at WHERE id=request_id AND lease_until>resume_at;
   END IF;
   IF FOUND THEN advanced:=advanced||jsonb_build_array(wait);
   ELSE unchanged:=unchanged||jsonb_build_array(jsonb_build_object('kind',wait->>'kind','id',wait->>'id','reason','already_due')); END IF;
  END LOOP;
 END IF;
 result:=jsonb_build_object('version',1,'operation_id',operation,'applied_at',stamp,'recovery_hold',NOT resume,
 'hold_generation',new_generation,'resolved_legacy',resolve_legacy,'resolved_owners',removed,'advanced_waits',advanced,'unchanged_waits',unchanged);
 INSERT INTO openlegal_admin.provider_recovery_audit(operation_id,plan,actor,database_actor,actual_writers_stopped_at,applied_at,before_state,result)
 VALUES(operation,p_plan,request->>'actor',session_user,stop_at,stamp,before_state,result);
 PERFORM pg_notify('openlegal_collection','');
 RETURN result;
END $$;

-- Refuse EXECUTE by PUBLIC for internal helpers and every supported entrypoint.
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA openlegal_admin FROM PUBLIC;
-- Existing runtime flags remain unchanged. This code is finite vocabulary only.
ALTER TABLE openlegal.collection_request DROP CONSTRAINT collection_request_reason_check,
 ADD CONSTRAINT collection_request_reason_check CHECK(reason IN (
 'ambiguous','source_inventory_incomplete','not_found','source_data_invalid','source_unavailable','download_failed',
 'identity_conflict','worker_failed','worker_lost','collection_pending','already_fresh','collection_already_in_progress',
 'head_observation_superseded','publication_superseded','no_matches','multiple_skip_reasons',
 'provider_daily_limit','provider_suspended','provider_response_uncertain','provider_retry_after',
 'operation_attempt_limit','provider_admission_wait','capacity_wait','provider_recovery_hold'));
