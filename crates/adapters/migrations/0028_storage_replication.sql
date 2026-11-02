-- Optional strict replication. A DBA provisions the dedicated NOLOGIN owner,
-- its narrowly required catalog-function privileges and named EXECUTE grants.
-- This migration creates no LOGIN/REPLICATION roles and changes no PG settings.
CREATE SCHEMA openlegal_replication;
REVOKE ALL ON SCHEMA openlegal_replication FROM PUBLIC;
CREATE TABLE openlegal_replication.control (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 managed_epoch uuid NOT NULL CHECK(managed_epoch<>'00000000-0000-0000-0000-000000000000'),
 primary_system_identifier text NOT NULL CHECK(primary_system_identifier ~ '^[0-9]{1,20}$'),
 timeline bigint NOT NULL CHECK(timeline BETWEEN 1 AND 4294967295),
 standby_name text NOT NULL CHECK(standby_name ~ '^[a-z][a-z0-9_]{0,62}$'),
 standby_role name NOT NULL,
 standby_addr inet NOT NULL,
 standby_client_dn text NOT NULL CHECK(octet_length(standby_client_dn) BETWEEN 1 AND 1024),
 standby_issuer_dn text NOT NULL CHECK(octet_length(standby_issuer_dn) BETWEEN 1 AND 1024),
 standby_client_serial numeric(60,0) NOT NULL CHECK(standby_client_serial>0),
 blob_binding_sha256 text NOT NULL CHECK(blob_binding_sha256 ~ '^[0-9a-f]{64}$')
);
REVOKE ALL ON ALL TABLES IN SCHEMA openlegal_replication FROM PUBLIC;
-- A nonsecret boolean is deliberately available to legacy runtime roles. No
-- USAGE on the replication schema or observer/catalog privilege is implied.
CREATE FUNCTION openlegal.replication_required() RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT EXISTS(SELECT 1 FROM openlegal_replication.control WHERE singleton)
$$;
GRANT EXECUTE ON FUNCTION openlegal.replication_required() TO PUBLIC;
CREATE FUNCTION openlegal_replication.capture_prefix() RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c openlegal_replication.control%ROWTYPE; p record; s record; checkpoint record;
BEGIN
 SELECT * INTO STRICT c FROM openlegal_replication.control WHERE singleton;
 IF pg_is_in_recovery() OR current_setting('synchronous_commit')<>'on'
    OR current_setting('fsync')<>'on' OR current_setting('full_page_writes')<>'on'
    OR current_setting('synchronous_standby_names')<>format('FIRST 1 (%s)',c.standby_name)
 THEN RAISE EXCEPTION 'replication binding unavailable' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT p FROM pg_control_system();
 SELECT * INTO STRICT checkpoint FROM pg_control_checkpoint();
 IF p.system_identifier::text<>c.primary_system_identifier OR checkpoint.timeline_id<>c.timeline
 THEN RAISE EXCEPTION 'replication primary changed' USING ERRCODE='55000'; END IF;
 -- Clear per-transaction statistics snapshots before every observation. No
 -- mutable connection information, query text or credentials are returned.
 PERFORM pg_stat_clear_snapshot();
 SELECT r.pid,r.backend_start,r.flush_lsn INTO STRICT s
 FROM pg_stat_replication r JOIN pg_stat_ssl ssl USING(pid)
 WHERE r.application_name=c.standby_name AND r.usename=c.standby_role
 AND r.client_addr=c.standby_addr AND r.state='streaming' AND r.sync_state='sync'
 AND r.flush_lsn IS NOT NULL AND ssl.ssl AND ssl.client_dn=c.standby_client_dn
 AND ssl.issuer_dn=c.standby_issuer_dn AND ssl.client_serial=c.standby_client_serial;
 RETURN jsonb_build_object('managed_epoch',c.managed_epoch,'blob_binding_sha256',c.blob_binding_sha256,
 'primary_system_identifier',p.system_identifier::text,'timeline',checkpoint.timeline_id,
 'postmaster_started_at',pg_postmaster_start_time()::text,
 'standby_name',c.standby_name,'standby_role',c.standby_role::text,
 'standby_addr',c.standby_addr::text,'standby_client_dn',c.standby_client_dn,
 'standby_issuer_dn',c.standby_issuer_dn,'standby_client_serial',c.standby_client_serial::text,'sender_pid',s.pid,
 -- Every completed authoritative synchronous COMMIT is covered by this local
 -- durable cut. Idle non-commit WAL may leave an unflushed partial page; it is
 -- not a publication acknowledgement and must not become an unreachable target.
 'sender_started_at',s.backend_start::text,'prefix_lsn',pg_current_wal_flush_lsn()::text,
 'flush_lsn',s.flush_lsn::text);
END $$;
CREATE FUNCTION openlegal_replication.observe_flush() RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT openlegal_replication.capture_prefix()
$$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA openlegal_replication FROM PUBLIC;

-- Canary accounting is independent of UTC daily counters and all request modes.
-- A closed record remains the required operation: ending never enables bypass.
CREATE TABLE openlegal_admin.provider_canary (
 id uuid PRIMARY KEY CHECK(id<>'00000000-0000-0000-0000-000000000000'),
 operation_id uuid NOT NULL UNIQUE REFERENCES openlegal_admin.provider_recovery_audit(operation_id),
 consumed integer NOT NULL DEFAULT 0 CHECK(consumed BETWEEN 0 AND 100),
 started_at timestamptz NOT NULL,
 deadline timestamptz NOT NULL,
 last_observed_at timestamptz NOT NULL,
 ended_at timestamptz,
 CHECK(deadline=started_at+interval '6 hours'),
 CHECK(last_observed_at>=started_at)
);
CREATE TABLE openlegal_admin.provider_canary_control (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 required_id uuid REFERENCES openlegal_admin.provider_canary(id)
);
INSERT INTO openlegal_admin.provider_canary_control DEFAULT VALUES;
CREATE TABLE openlegal_admin.provider_canary_audit (
 operation_id uuid NOT NULL REFERENCES openlegal_admin.provider_recovery_audit(operation_id),
 canary_id uuid NOT NULL REFERENCES openlegal_admin.provider_canary(id),
 action text NOT NULL CHECK(action IN ('init','close')),
 applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 database_actor name NOT NULL DEFAULT session_user,
 result jsonb NOT NULL,
 PRIMARY KEY(operation_id,canary_id,action)
);
REVOKE ALL ON openlegal_admin.provider_canary,openlegal_admin.provider_canary_control,
 openlegal_admin.provider_canary_audit FROM PUBLIC;

CREATE FUNCTION openlegal_admin.canary_init(p_operation uuid,p_id uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c openlegal_admin.provider_control%ROWTYPE; existing openlegal_admin.provider_canary%ROWTYPE;
 stamp timestamptz:=clock_timestamp(); current_id uuid;
BEGIN
 -- Same lock order as admission: budget, provider control, canary control/row.
 PERFORM 1 FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT c FROM openlegal_admin.provider_control WHERE singleton FOR UPDATE;
 IF NOT c.recovery_hold OR NOT EXISTS(SELECT 1 FROM openlegal_admin.provider_recovery_audit a
 WHERE a.operation_id=p_operation AND a.result->>'hold_generation'=c.generation::text
 AND a.result->>'recovery_hold'='true')
 OR EXISTS(SELECT 1 FROM openlegal.provider_request_admission)
 THEN RAISE EXCEPTION 'canary requires audited held operation' USING ERRCODE='55000'; END IF;
 SELECT required_id INTO current_id FROM openlegal_admin.provider_canary_control WHERE singleton FOR UPDATE;
 SELECT * INTO existing FROM openlegal_admin.provider_canary WHERE id=p_id FOR UPDATE;
 IF FOUND THEN
  IF existing.operation_id<>p_operation OR current_id IS DISTINCT FROM p_id OR existing.ended_at IS NOT NULL
  THEN RAISE EXCEPTION 'canary identity changed' USING ERRCODE='55000'; END IF;
 ELSE
  IF current_id IS NOT NULL AND EXISTS(SELECT 1 FROM openlegal_admin.provider_canary WHERE id=current_id AND ended_at IS NULL)
  THEN RAISE EXCEPTION 'canary already active' USING ERRCODE='55000'; END IF;
  INSERT INTO openlegal_admin.provider_canary(id,operation_id,started_at,deadline,last_observed_at)
  VALUES(p_id,p_operation,stamp,stamp+interval '6 hours',stamp);
  UPDATE openlegal_admin.provider_canary_control SET required_id=p_id WHERE singleton;
  INSERT INTO openlegal_admin.provider_canary_audit(operation_id,canary_id,action,result)
  VALUES(p_operation,p_id,'init',jsonb_build_object('limit',100,'consumed',0,'deadline',stamp+interval '6 hours'));
 END IF;
 RETURN (SELECT jsonb_build_object('id',id,'consumed',consumed,'deadline',deadline,'active',ended_at IS NULL)
 FROM openlegal_admin.provider_canary WHERE id=p_id);
END $$;

CREATE FUNCTION openlegal_admin.canary_consume(p_id uuid) RETURNS timestamptz
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE required uuid; c openlegal_admin.provider_canary%ROWTYPE; stamp timestamptz:=clock_timestamp();
BEGIN
 -- Reentrant row lock proves that every caller participates in budget order.
 PERFORM 1 FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE;
 SELECT required_id INTO STRICT required FROM openlegal_admin.provider_canary_control WHERE singleton FOR UPDATE;
 IF required IS NULL AND p_id IS NULL THEN RETURN NULL; END IF;
 IF required IS DISTINCT FROM p_id OR p_id IS NULL
 THEN RAISE EXCEPTION 'canary identity required' USING ERRCODE='55000'; END IF;
 SELECT * INTO STRICT c FROM openlegal_admin.provider_canary WHERE id=p_id FOR UPDATE;
 IF c.ended_at IS NOT NULL OR c.consumed>=100 OR stamp>=c.deadline
 THEN RAISE EXCEPTION 'canary exhausted or expired' USING ERRCODE='54000'; END IF;
 IF stamp<c.last_observed_at
 THEN RAISE EXCEPTION 'canary clock moved backwards' USING ERRCODE='55000'; END IF;
 UPDATE openlegal_admin.provider_canary SET consumed=consumed+1,last_observed_at=stamp WHERE id=p_id;
 RETURN c.deadline;
END $$;

CREATE FUNCTION openlegal_admin.canary_close(p_operation uuid,p_id uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c openlegal_admin.provider_control%ROWTYPE;
BEGIN
 PERFORM 1 FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT c FROM openlegal_admin.provider_control WHERE singleton FOR UPDATE;
 IF NOT c.recovery_hold OR NOT EXISTS(SELECT 1 FROM openlegal_admin.provider_recovery_audit a
 WHERE a.operation_id=p_operation AND a.result->>'hold_generation'=c.generation::text
 AND a.result->>'recovery_hold'='true')
 OR EXISTS(SELECT 1 FROM openlegal.provider_request_admission)
 THEN RAISE EXCEPTION 'canary close requires audited hold and drain' USING ERRCODE='55000'; END IF;
 PERFORM 1 FROM openlegal_admin.provider_canary_control WHERE singleton AND required_id=p_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'canary identity changed' USING ERRCODE='55000'; END IF;
 UPDATE openlegal_admin.provider_canary SET ended_at=COALESCE(ended_at,clock_timestamp()) WHERE id=p_id;
 INSERT INTO openlegal_admin.provider_canary_audit(operation_id,canary_id,action,result)
 SELECT p_operation,p_id,'close',jsonb_build_object('consumed',consumed,'ended_at',ended_at)
 FROM openlegal_admin.provider_canary WHERE id=p_id ON CONFLICT DO NOTHING;
 RETURN (SELECT jsonb_build_object('id',id,'consumed',consumed,'deadline',deadline,'active',false)
 FROM openlegal_admin.provider_canary WHERE id=p_id);
END $$;
REVOKE ALL ON FUNCTION openlegal_admin.canary_init(uuid,uuid),
 openlegal_admin.canary_consume(uuid),openlegal_admin.canary_close(uuid,uuid) FROM PUBLIC;
