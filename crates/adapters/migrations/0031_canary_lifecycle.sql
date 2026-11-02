-- Optional, explicit recovery-only canary lifecycle. No LOGIN, credentials,
-- automatic recovery, provider traffic or privilege grants are created here.
CREATE TABLE openlegal_checkpoint.canary_lifecycle (
 canary_id uuid PRIMARY KEY REFERENCES openlegal_admin.provider_canary(id),
 recovery_operation_id uuid NOT NULL UNIQUE REFERENCES openlegal_admin.provider_recovery_audit(operation_id),
 intent_sha256 text NOT NULL CHECK(intent_sha256 ~ '^[0-9a-f]{64}$'),
 managed_epoch uuid NOT NULL,
 blob_binding_sha256 text NOT NULL CHECK(blob_binding_sha256 ~ '^[0-9a-f]{64}$'),
 original_hold_generation uuid NOT NULL,
 released_generation uuid NOT NULL,
 activated_at timestamptz NOT NULL,
 activation_result jsonb NOT NULL,
 final_checkpoint_operation uuid UNIQUE REFERENCES openlegal_checkpoint.operation(operation_id),
 final_intent_sha256 text,
 closed_at timestamptz,
 finish_result jsonb,
 CHECK((closed_at IS NULL)=(finish_result IS NULL)),
 CHECK((closed_at IS NULL)=(final_checkpoint_operation IS NULL)),
 CHECK((closed_at IS NULL)=(final_intent_sha256 IS NULL)),
 CHECK(final_intent_sha256 IS NULL OR final_intent_sha256 ~ '^[0-9a-f]{64}$')
);
REVOKE ALL ON openlegal_checkpoint.canary_lifecycle FROM PUBLIC;

CREATE FUNCTION openlegal_checkpoint.activate_canary(
 p_recovery uuid,p_canary uuid,p_intent_sha256 text,p_expected_hold_generation uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c openlegal_admin.provider_control%ROWTYPE;
 b openlegal.provider_request_budget%ROWTYPE;
 r openlegal_replication.control%ROWTYPE;
 k openlegal_admin.provider_canary%ROWTYPE;
 slot integer; released uuid; result jsonb;
BEGIN
 IF p_recovery IS NULL OR p_recovery='00000000-0000-0000-0000-000000000000'
 OR p_canary IS NULL OR p_canary='00000000-0000-0000-0000-000000000000'
 OR p_expected_hold_generation IS NULL
 OR p_intent_sha256 IS NULL OR p_intent_sha256 !~ '^[0-9a-f]{64}$'
 THEN RAISE EXCEPTION 'invalid canary activation intent' USING ERRCODE='22023'; END IF;
 PERFORM singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'corpus control unavailable' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT b FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT c FROM openlegal_admin.provider_control WHERE singleton FOR UPDATE;
 FOR slot IN 0..16 LOOP
  IF NOT pg_try_advisory_xact_lock(1869376611,1818326864+slot) THEN
   RAISE EXCEPTION 'provider owner remains live' USING ERRCODE='55P03'; END IF;
 END LOOP;
 IF NOT c.recovery_hold OR c.generation<>p_expected_hold_generation
 OR b.unresolved_response OR b.admission_owner IS NOT NULL
 OR EXISTS(SELECT 1 FROM openlegal.provider_request_admission)
 OR NOT EXISTS(SELECT 1 FROM openlegal_admin.provider_recovery_audit a
  WHERE a.operation_id=p_recovery AND a.result->>'hold_generation'=c.generation::text
   AND a.result->>'recovery_hold'='true')
 THEN RAISE EXCEPTION 'canary requires exact audited clean hold' USING ERRCODE='55000'; END IF;
 IF EXISTS(SELECT 1 FROM openlegal_checkpoint.canary_lifecycle WHERE canary_id=p_canary)
 OR EXISTS(SELECT 1 FROM openlegal_admin.provider_canary WHERE id=p_canary)
 THEN RAISE EXCEPTION 'canary activation requires readback' USING ERRCODE='40001'; END IF;
 SELECT * INTO STRICT r FROM openlegal_replication.control WHERE singleton FOR SHARE;
 PERFORM openlegal_replication.capture_prefix();
 -- The existing audited init supplies the original six-hour deadline and cap.
 -- It is atomic with releasing this exact held generation. No writer stop or
 -- provider audit is fabricated here; audited setup precedes serving startup.
 PERFORM openlegal_admin.canary_init(p_recovery,p_canary);
 SELECT * INTO STRICT k FROM openlegal_admin.provider_canary WHERE id=p_canary FOR UPDATE;
 released:=pg_catalog.uuidv7();
 UPDATE openlegal_admin.provider_control SET recovery_hold=false,generation=released,
  hold_started_at=NULL WHERE singleton;
 result:=jsonb_build_object('canary_id',p_canary,'recovery_operation_id',p_recovery,
  'intent_sha256',p_intent_sha256,'managed_epoch',r.managed_epoch,
  'blob_binding_sha256',r.blob_binding_sha256,'original_hold_generation',c.generation,
  'released_generation',released,'started_at',k.started_at,'deadline',k.deadline,
  'consumed',k.consumed,'recovery_hold',false);
 INSERT INTO openlegal_checkpoint.canary_lifecycle(canary_id,recovery_operation_id,
  intent_sha256,managed_epoch,blob_binding_sha256,original_hold_generation,
  released_generation,activated_at,activation_result)
 VALUES(p_canary,p_recovery,p_intent_sha256,r.managed_epoch,r.blob_binding_sha256,
  c.generation,released,clock_timestamp(),result);
 RETURN result;
END $$;

CREATE FUNCTION openlegal_checkpoint.finish_canary(
 p_checkpoint uuid,p_intent_sha256 text,p_canary uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c openlegal_admin.provider_control%ROWTYPE;
 b openlegal.provider_request_budget%ROWTYPE;
 o openlegal_checkpoint.operation%ROWTYPE;
 l openlegal_checkpoint.canary_lifecycle%ROWTYPE;
 k openlegal_admin.provider_canary%ROWTYPE;
 r openlegal_replication.control%ROWTYPE;
 slot integer; stamp timestamptz; result jsonb;
BEGIN
 IF p_checkpoint IS NULL OR p_checkpoint='00000000-0000-0000-0000-000000000000'
 OR p_canary IS NULL OR p_canary='00000000-0000-0000-0000-000000000000'
 OR p_intent_sha256 IS NULL OR p_intent_sha256 !~ '^[0-9a-f]{64}$'
 THEN RAISE EXCEPTION 'invalid canary finish intent' USING ERRCODE='22023'; END IF;
 PERFORM singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'corpus control unavailable' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT b FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT c FROM openlegal_admin.provider_control WHERE singleton FOR UPDATE;
 FOR slot IN 0..16 LOOP
  IF NOT pg_try_advisory_xact_lock(1869376611,1818326864+slot) THEN
   RAISE EXCEPTION 'provider owner remains live' USING ERRCODE='55P03'; END IF;
 END LOOP;
 SELECT * INTO STRICT o FROM openlegal_checkpoint.operation WHERE operation_id=p_checkpoint FOR UPDATE;
 IF NOT o.owned OR o.released_at IS NOT NULL OR o.intent_sha256<>p_intent_sha256
 OR NOT c.recovery_hold OR c.generation<>o.installed_generation
 OR b.unresolved_response OR b.admission_owner IS NOT NULL
 OR EXISTS(SELECT 1 FROM openlegal.provider_request_admission)
 OR openlegal_checkpoint.state() IS DISTINCT FROM o.clean_state
 THEN RAISE EXCEPTION 'canary finish requires owned clean hold' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT l FROM openlegal_checkpoint.canary_lifecycle WHERE canary_id=p_canary FOR UPDATE;
 IF l.closed_at IS NOT NULL THEN
  RAISE EXCEPTION 'canary finish requires readback' USING ERRCODE='40001'; END IF;
 SELECT * INTO STRICT r FROM openlegal_replication.control WHERE singleton FOR SHARE;
 IF r.managed_epoch<>l.managed_epoch OR r.blob_binding_sha256<>l.blob_binding_sha256 THEN
  RAISE EXCEPTION 'canary epoch changed' USING ERRCODE='55000'; END IF;
 PERFORM openlegal_replication.capture_prefix();
 PERFORM singleton FROM openlegal_admin.provider_canary_control
  WHERE singleton AND required_id=p_canary FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'canary identity changed' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT k FROM openlegal_admin.provider_canary WHERE id=p_canary FOR UPDATE;
 stamp:=clock_timestamp();
 IF k.operation_id<>l.recovery_operation_id OR k.ended_at IS NOT NULL OR stamp<k.deadline
 OR k.started_at IS DISTINCT FROM (l.activation_result->>'started_at')::timestamptz
 OR k.deadline IS DISTINCT FROM (l.activation_result->>'deadline')::timestamptz
 THEN RAISE EXCEPTION 'canary window incomplete or changed' USING ERRCODE='55000'; END IF;
 UPDATE openlegal_admin.provider_canary SET ended_at=stamp WHERE id=p_canary;
 result:=jsonb_build_object('canary_id',p_canary,'recovery_operation_id',l.recovery_operation_id,
  'final_checkpoint_operation',p_checkpoint,'intent_sha256',p_intent_sha256,
  'managed_epoch',l.managed_epoch,'blob_binding_sha256',l.blob_binding_sha256,
  'hold_generation',c.generation,'started_at',k.started_at,'deadline',k.deadline,
  'closed_at',stamp,'consumed',k.consumed,'recovery_hold',true,
  'physical_closure_and_publication_acceptance','separate_required_evidence');
 INSERT INTO openlegal_admin.provider_canary_audit(operation_id,canary_id,action,result)
 VALUES(l.recovery_operation_id,p_canary,'close',result);
 UPDATE openlegal_checkpoint.canary_lifecycle SET final_checkpoint_operation=p_checkpoint,
  final_intent_sha256=p_intent_sha256,closed_at=stamp,finish_result=result WHERE canary_id=p_canary;
 -- Keep the final hold and required_id. No function here enables unrestricted
 -- traffic, clears uncertainty, changes policy, or refunds charged attempts.
 RETURN result;
END $$;

CREATE FUNCTION openlegal_checkpoint.canary_readback(p_canary uuid) RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('activation',activation_result,'finish',finish_result)
 FROM openlegal_checkpoint.canary_lifecycle WHERE canary_id=p_canary
$$;
REVOKE ALL ON FUNCTION openlegal_checkpoint.activate_canary(uuid,uuid,text,uuid),
 openlegal_checkpoint.finish_canary(uuid,text,uuid),openlegal_checkpoint.canary_readback(uuid) FROM PUBLIC;
