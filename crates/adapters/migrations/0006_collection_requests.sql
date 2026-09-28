-- Explicit public collection requests are separate from read-only lookups.
-- The key coalesces equal requests for one day; payload is cleared at settlement.
CREATE TABLE openlegal.collection_request (
 id uuid PRIMARY KEY DEFAULT pg_catalog.uuidv7(),
 request_key text NOT NULL UNIQUE CHECK(request_key ~ '^[0-9a-f]{64}$'),
 payload jsonb NOT NULL,
 status text NOT NULL CHECK(status IN ('queued','launching','running','done','skipped','deferred','failed')),
 created_at bigint NOT NULL,
 expires_at bigint NOT NULL,
 lease_until bigint,
 job_name text,
 CHECK(expires_at > created_at)
);
CREATE INDEX collection_request_ready ON openlegal.collection_request(created_at)
 WHERE status = 'queued';

ALTER TABLE openlegal.provider_request_budget
 ADD COLUMN on_demand_used integer NOT NULL DEFAULT 0
 CHECK(on_demand_used BETWEEN 0 AND 1000);

ALTER TABLE openlegal.corpus_control
 ADD COLUMN collection_scheduler_seen_at bigint NOT NULL DEFAULT 0;
