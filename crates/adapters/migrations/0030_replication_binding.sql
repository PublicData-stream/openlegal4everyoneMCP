-- Explicit first seed/manual rebind only. No LOGIN, credential, configuration
-- relaxation or automatic promotion is created here. A separate recovery role
-- receives named EXECUTE after the physical two-copy intent is reviewed.
CREATE FUNCTION openlegal_checkpoint.bind_replica(
 p_operation uuid,p_intent_sha256 text,p_expected_old jsonb,p_new jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE old openlegal_replication.control%ROWTYPE;
 expected openlegal_replication.control%ROWTYPE;
 proposed openlegal_replication.control%ROWTYPE;
 control openlegal_admin.provider_control%ROWTYPE;
 had_old boolean; proof jsonb; slot integer;
 keys text[]:=ARRAY['managed_epoch','primary_system_identifier','timeline','standby_name',
  'standby_role','standby_addr','standby_client_dn','standby_issuer_dn',
  'standby_client_serial','blob_binding_sha256'];
BEGIN
 IF p_operation IS NULL OR p_operation='00000000-0000-0000-0000-000000000000'
 OR p_intent_sha256 IS NULL OR p_intent_sha256 !~ '^[0-9a-f]{64}$'
 OR p_new IS NULL OR jsonb_typeof(p_new)<>'object'
 THEN RAISE EXCEPTION 'invalid replication binding intent' USING ERRCODE='22023'; END IF;
 IF NOT p_new ?& keys OR (SELECT count(*) FROM jsonb_object_keys(p_new))<>cardinality(keys)
 THEN RAISE EXCEPTION 'invalid replication binding fields' USING ERRCODE='22023'; END IF;
 -- Provider mutation/admission lock order. This never clears provider state,
 -- charges or an uncertain ledger. All old physical writers must independently
 -- be fenced by the source-pinned operator before invoking this function.
 PERFORM singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'corpus control unavailable' USING ERRCODE='55000'; END IF;
 PERFORM singleton FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'provider budget unavailable' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT control FROM openlegal_admin.provider_control WHERE singleton FOR UPDATE;
 IF NOT control.recovery_hold THEN
  RAISE EXCEPTION 'replication binding requires held provider' USING ERRCODE='55000'; END IF;
 FOR slot IN 0..16 LOOP
  IF NOT pg_try_advisory_xact_lock(1869376611,1818326864+slot) THEN
   RAISE EXCEPTION 'provider owner remains live' USING ERRCODE='55P03'; END IF;
 END LOOP;
 SELECT * INTO old FROM openlegal_replication.control WHERE singleton FOR UPDATE;
 had_old:=FOUND;
 IF p_expected_old IS NULL OR jsonb_typeof(p_expected_old)='null' THEN
  IF had_old THEN RAISE EXCEPTION 'replication binding changed' USING ERRCODE='40001'; END IF;
 ELSE
  IF jsonb_typeof(p_expected_old)<>'object' OR NOT p_expected_old ?& keys
   OR (SELECT count(*) FROM jsonb_object_keys(p_expected_old))<>cardinality(keys)
  THEN RAISE EXCEPTION 'invalid previous replication binding' USING ERRCODE='22023'; END IF;
  SELECT * INTO expected FROM jsonb_populate_record(NULL::openlegal_replication.control,
    p_expected_old||jsonb_build_object('singleton',true));
  IF NOT had_old OR old IS DISTINCT FROM expected THEN
   RAISE EXCEPTION 'replication binding changed' USING ERRCODE='40001'; END IF;
 END IF;
 SELECT * INTO proposed FROM jsonb_populate_record(NULL::openlegal_replication.control,
   p_new||jsonb_build_object('singleton',true));
 IF had_old AND (proposed.managed_epoch=old.managed_epoch
   OR proposed.primary_system_identifier<>old.primary_system_identifier OR proposed.timeline<=old.timeline)
 THEN RAISE EXCEPTION 'manual binding requires new epoch and promoted timeline' USING ERRCODE='55000'; END IF;
 -- Table constraints retain exact UUID/hash/identifier/serial domains. This
 -- change is uncommitted and invisible to other processes; any observer error
 -- below rolls it back. The fixed observer checks the actual current system ID,
 -- timeline, synchronous settings and authenticated streaming/sync peer.
 IF had_old THEN
  UPDATE openlegal_replication.control SET managed_epoch=proposed.managed_epoch,
   primary_system_identifier=proposed.primary_system_identifier,timeline=proposed.timeline,
   standby_name=proposed.standby_name,standby_role=proposed.standby_role,
   standby_addr=proposed.standby_addr,standby_client_dn=proposed.standby_client_dn,
   standby_issuer_dn=proposed.standby_issuer_dn,standby_client_serial=proposed.standby_client_serial,
   blob_binding_sha256=proposed.blob_binding_sha256 WHERE singleton;
 ELSE
  INSERT INTO openlegal_replication.control SELECT (proposed).*;
 END IF;
 proof:=openlegal_replication.capture_prefix();
 RETURN jsonb_build_object('operation_id',p_operation,'intent_sha256',p_intent_sha256,
  'previous_binding',CASE WHEN had_old THEN to_jsonb(old)-'singleton' ELSE NULL END,
  'new_binding',to_jsonb(proposed)-'singleton','peer_before_commit',proof);
END $$;
REVOKE ALL ON FUNCTION openlegal_checkpoint.bind_replica(uuid,text,jsonb,jsonb) FROM PUBLIC;

-- Read-only startup observation before the sole binding mutation. The epoch and
-- Blob binding are explicitly PROPOSED intent, never a claim that control was
-- installed. Provision this under the existing NOLOGIN replication observer,
-- whose fixed catalog privileges already support capture_prefix().
CREATE FUNCTION openlegal_replication.observe_candidate(p_new jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE proposed openlegal_replication.control%ROWTYPE; p record; checkpoint record; s record;
 keys text[]:=ARRAY['managed_epoch','primary_system_identifier','timeline','standby_name',
  'standby_role','standby_addr','standby_client_dn','standby_issuer_dn',
  'standby_client_serial','blob_binding_sha256'];
BEGIN
 IF p_new IS NULL OR jsonb_typeof(p_new)<>'object' OR NOT p_new ?& keys
 OR (SELECT count(*) FROM jsonb_object_keys(p_new))<>cardinality(keys)
 THEN RAISE EXCEPTION 'invalid candidate binding fields' USING ERRCODE='22023'; END IF;
 SELECT * INTO proposed FROM jsonb_populate_record(NULL::openlegal_replication.control,
   p_new||jsonb_build_object('singleton',true));
 IF pg_is_in_recovery() OR current_setting('synchronous_commit')<>'on'
 OR current_setting('fsync')<>'on' OR current_setting('full_page_writes')<>'on'
 OR current_setting('synchronous_standby_names')<>format('FIRST 1 (%s)',proposed.standby_name)
 THEN RAISE EXCEPTION 'candidate replication unavailable' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT p FROM pg_control_system();
 SELECT * INTO STRICT checkpoint FROM pg_control_checkpoint();
 IF p.system_identifier::text IS DISTINCT FROM proposed.primary_system_identifier
 OR checkpoint.timeline_id IS DISTINCT FROM proposed.timeline
 THEN RAISE EXCEPTION 'candidate primary changed' USING ERRCODE='55000'; END IF;
 PERFORM pg_stat_clear_snapshot();
 SELECT r.pid,r.backend_start,r.flush_lsn INTO STRICT s
 FROM pg_stat_replication r JOIN pg_stat_ssl ssl USING(pid)
 WHERE r.application_name=proposed.standby_name AND r.usename=proposed.standby_role
 AND r.client_addr=proposed.standby_addr AND r.state='streaming' AND r.sync_state='sync'
 AND r.flush_lsn IS NOT NULL AND ssl.ssl AND ssl.client_dn=proposed.standby_client_dn
 AND ssl.issuer_dn=proposed.standby_issuer_dn AND ssl.client_serial=proposed.standby_client_serial;
 RETURN jsonb_build_object('kind','openlegal_replication_candidate_peer','proposed_binding',p_new,
  'candidate',jsonb_build_object('primary_system_identifier',p.system_identifier::text,
   'timeline',checkpoint.timeline_id,'postmaster_started_at',pg_postmaster_start_time()::text,
   'standby_name',proposed.standby_name,'standby_role',proposed.standby_role::text,
   'standby_addr',proposed.standby_addr::text,'standby_client_dn',proposed.standby_client_dn,
   'standby_issuer_dn',proposed.standby_issuer_dn,'standby_client_serial',proposed.standby_client_serial::text,
   'sender_pid',s.pid,'sender_started_at',s.backend_start::text,
   'prefix_lsn',pg_current_wal_flush_lsn()::text,'flush_lsn',s.flush_lsn::text));
END $$;
REVOKE ALL ON FUNCTION openlegal_replication.observe_candidate(jsonb) FROM PUBLIC;
