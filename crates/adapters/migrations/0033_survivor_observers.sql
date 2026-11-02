-- Standby recovery observers. No role/credential/control mutation.
-- The dedicated replication NOLOGIN owner receives its existing fixed catalog
-- function privileges, SELECT control, and pg_read_all_settings for one effective
-- data-directory value. The checkpoint NOLOGIN owner receives fixed EXECUTE on
-- these observers/state and existing retained metadata functions. Named recovery
-- callers get only the explicit EXECUTEs; never PUBLIC or broad LOGIN catalogs.
CREATE FUNCTION openlegal_replication.survivor_position() RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
 SET TimeZone='UTC' SET DateStyle='ISO,YMD' AS $$
DECLARE c openlegal_replication.control%ROWTYPE; system_id text; timeline_id bigint;
BEGIN
 IF NOT pg_is_in_recovery() THEN
  RAISE EXCEPTION 'survivor is not a standby' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT c FROM openlegal_replication.control WHERE singleton;
 SELECT system_identifier::text INTO STRICT system_id FROM pg_control_system();
 SELECT checkpoint.timeline_id INTO STRICT timeline_id FROM pg_control_checkpoint() checkpoint;
 IF c.primary_system_identifier<>system_id OR c.timeline<>timeline_id THEN
  RAISE EXCEPTION 'survivor lineage changed' USING ERRCODE='55000'; END IF;
 RETURN jsonb_build_object('schema_version',1,'kind','openlegal_survivor_position',
  'in_recovery',true,'system_identifier',system_id,'timeline',timeline_id,
  'postmaster_started_at',pg_postmaster_start_time()::text,
  'data_directory',current_setting('data_directory'),
  'control',(to_jsonb(c)-'singleton')||jsonb_build_object(
    'standby_addr',host(c.standby_addr),'standby_client_serial',c.standby_client_serial::text),
  'receive_lsn',pg_last_wal_receive_lsn()::text,'replay_lsn',pg_last_wal_replay_lsn()::text,
  'replay_paused',pg_is_wal_replay_paused(),
  -- Never filter masked status fields to infer absence. No conninfo/query text.
  'wal_receiver_count',(SELECT count(*) FROM pg_stat_wal_receiver));
END $$;
REVOKE ALL ON FUNCTION openlegal_replication.survivor_position() FROM PUBLIC;

CREATE FUNCTION openlegal_checkpoint.provider_survivor_state() RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog
 SET TimeZone='UTC' SET DateStyle='ISO,YMD' AS $$
DECLARE provider jsonb; control jsonb;
BEGIN
 IF NOT pg_is_in_recovery() THEN
  RAISE EXCEPTION 'survivor is not a standby' USING ERRCODE='55000'; END IF;
 provider:=openlegal_checkpoint.state();
 SELECT to_jsonb(c) INTO STRICT control FROM openlegal_admin.provider_control c WHERE singleton;
 IF provider IS NULL THEN
  RAISE EXCEPTION 'provider metadata unavailable' USING ERRCODE='55000'; END IF;
 -- Primary advisory locks are not WAL-replicated. Their absence here cannot
 -- certify clean ownership or authorize deleting/adopting a ledger entry.
 RETURN jsonb_build_object('state',provider,'hold_control',control,
  'lock_provenance','primary_advisory_locks_not_observable_on_standby');
END $$;
REVOKE ALL ON FUNCTION openlegal_checkpoint.provider_survivor_state() FROM PUBLIC;

CREATE FUNCTION openlegal_checkpoint.survivor_snapshot() RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
 SET TimeZone='UTC' SET DateStyle='ISO,YMD' SET IntervalStyle='iso_8601'
 SET bytea_output='hex' SET extra_float_digits=3 AS $$
DECLARE before_position jsonb; after_position jsonb; tables jsonb; refs jsonb;
 provider jsonb; storage jsonb;
BEGIN
 IF NOT pg_is_in_recovery() OR current_setting('transaction_read_only')<>'on'
 OR current_setting('transaction_isolation')<>'repeatable read' THEN
  RAISE EXCEPTION 'survivor snapshot requires repeatable readonly transaction' USING ERRCODE='25006';
 END IF;
 before_position:=openlegal_replication.survivor_position();
 tables:=openlegal_checkpoint.retained_tables();
 refs:=openlegal_checkpoint.retained_references();
 provider:=openlegal_checkpoint.provider_survivor_state();
 storage:=openlegal_checkpoint.storage_metadata();
 after_position:=openlegal_replication.survivor_position();
 RETURN jsonb_build_object('position_before',before_position,'position_after',after_position,
  'transaction',jsonb_build_object('isolation',current_setting('transaction_isolation'),
    'read_only',current_setting('transaction_read_only')='on',
    'statement_timeout_ms',(extract(epoch FROM current_setting('statement_timeout')::interval)*1000)::bigint,
    'lock_timeout_ms',(extract(epoch FROM current_setting('lock_timeout')::interval)*1000)::bigint,
    'transaction_timeout_ms',(extract(epoch FROM current_setting('transaction_timeout')::interval)*1000)::bigint,
    'idle_in_transaction_session_timeout_ms',
      (extract(epoch FROM current_setting('idle_in_transaction_session_timeout')::interval)*1000)::bigint),
  'retained_tables',tables,'retained_references',refs,'provider',provider,'storage_metadata',storage);
END $$;
REVOKE ALL ON FUNCTION openlegal_checkpoint.survivor_snapshot() FROM PUBLIC;
