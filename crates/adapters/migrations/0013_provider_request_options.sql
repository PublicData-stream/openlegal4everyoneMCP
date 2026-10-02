-- Optional operator caps retain charged accounting and provider safety evidence.
ALTER TABLE openlegal.provider_request_budget
 DROP CONSTRAINT provider_request_budget_daily_used_check,
 DROP CONSTRAINT provider_request_budget_on_demand_used_check,
 DROP CONSTRAINT provider_request_budget_pilot_used_check,
 ALTER COLUMN daily_used TYPE bigint,
 ALTER COLUMN on_demand_used TYPE bigint,
 ALTER COLUMN pilot_used TYPE bigint,
 ADD CONSTRAINT provider_request_budget_daily_used_check CHECK(daily_used >= 0),
 ADD CONSTRAINT provider_request_budget_on_demand_used_check CHECK(on_demand_used >= 0),
 ADD CONSTRAINT provider_request_budget_pilot_used_check CHECK(pilot_used >= 0),
 ALTER COLUMN continuous_daily_limit DROP NOT NULL,
 ALTER COLUMN on_demand_daily_limit DROP NOT NULL,
 ADD COLUMN pilot_attempt_limit integer DEFAULT 100 CHECK(pilot_attempt_limit BETWEEN 1 AND 1000000),
 ADD COLUMN on_demand_attempt_limit integer DEFAULT 32 CHECK(on_demand_attempt_limit BETWEEN 1 AND 1000000),
 ADD COLUMN interval_ms integer NOT NULL DEFAULT 5001 CHECK(interval_ms BETWEEN 1 AND 3600001),
 ADD COLUMN pilot_timeout_secs bigint NOT NULL DEFAULT 1800 CHECK(pilot_timeout_secs BETWEEN 60 AND 86400),
 ADD COLUMN on_demand_timeout_secs bigint NOT NULL DEFAULT 7200 CHECK(on_demand_timeout_secs BETWEEN 60 AND 86400),
 ADD COLUMN pilot_duration_secs bigint CHECK(pilot_duration_secs BETWEEN 60 AND 86400),
 ADD COLUMN max_job_attempts integer NOT NULL DEFAULT 3 CHECK(max_job_attempts BETWEEN 1 AND 10);

-- Preserve the legacy one-millisecond margin and any already-started pilot window.
UPDATE openlegal.provider_request_budget
 SET interval_ms=min_interval_secs*1000+1,
     pilot_duration_secs=CASE WHEN pilot_started_at IS NOT NULL THEN 1800 ELSE NULL END;


-- Synthetic provider/origin budgets never depend on query or retained body keys.
CREATE TABLE openlegal.upstream_daily_budget (
 namespace text NOT NULL CHECK (octet_length(namespace) BETWEEN 1 AND 128),
 provider text NOT NULL CHECK (octet_length(provider) BETWEEN 1 AND 64),
 utc_day bigint NOT NULL DEFAULT -1,
 used bigint NOT NULL DEFAULT 0 CHECK (used >= 0),
 daily_limit bigint CHECK (daily_limit BETWEEN 1 AND 1000000),
 PRIMARY KEY (namespace, provider)
);
-- Private explicit-operation policy and original recovery ownership.
ALTER TABLE openlegal.collection_request
 ADD COLUMN operation_timeout_secs bigint NOT NULL DEFAULT 7200 CHECK(operation_timeout_secs BETWEEN 60 AND 86400),
 ADD COLUMN operation_attempt_limit bigint DEFAULT 32 CHECK(operation_attempt_limit BETWEEN 1 AND 1000000),
 ADD COLUMN launched_at bigint;
UPDATE openlegal.collection_request SET launched_at=COALESCE(lease_until-8100,created_at)
 WHERE status IN ('launching','running');
ALTER TABLE openlegal.corpus_job
 DROP CONSTRAINT corpus_job_attempts_check,
 ADD CONSTRAINT corpus_job_attempts_check CHECK(attempts BETWEEN 0 AND 10),
 ADD COLUMN explicit_request_id uuid,
 ADD COLUMN explicit_recovery_at bigint;
UPDATE openlegal.corpus_job SET explicit_recovery_at=created_at::bigint+8100
 WHERE source_metadata->>'collection_origin'='explicit';
