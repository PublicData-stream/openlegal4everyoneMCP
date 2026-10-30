-- A current-list ambiguity is an explicit sticky hold, never a guessed revision.
-- Existing captures, validation times, historical work and provider charges stay intact.
ALTER TABLE openlegal.corpus_object
 ADD COLUMN current_inventory_conflict boolean NOT NULL DEFAULT false,
 ADD CONSTRAINT current_inventory_conflict_pending
 CHECK(NOT current_inventory_conflict OR pending OR withdrawn);

-- Checkpoint identity includes an absent effective date without a sentinel.
-- Publication dates and row order cannot select a winner. Held objects preserve
-- their first conflicting evidence; later observations remain source observations.
CREATE TABLE openlegal.current_inventory_candidate (
 view_key text NOT NULL REFERENCES openlegal.provider_clone_view(view_key),
 cycle bigint NOT NULL CHECK(cycle>0),
 object_key text NOT NULL REFERENCES openlegal.corpus_object(object_key),
 revision_id text NOT NULL CHECK(octet_length(revision_id) BETWEEN 1 AND 256),
 effective_date text CHECK(effective_date ~ '^[0-9]{8}$'),
 page integer NOT NULL CHECK(page>0),
 observed_at openlegal.unix_seconds NOT NULL,
 source_observation_id text NOT NULL REFERENCES openlegal.corpus_source_observation(id),
 conflict_proof boolean NOT NULL DEFAULT false,
 UNIQUE NULLS NOT DISTINCT(view_key,cycle,object_key,revision_id,effective_date)
);
CREATE INDEX current_inventory_candidate_object
 ON openlegal.current_inventory_candidate(object_key,view_key,cycle);

-- Offline operator decisions are immutable, exact-manifest audit records.
CREATE TABLE openlegal.current_inventory_audit (
 operation_id uuid PRIMARY KEY,
 manifest_sha256 bytea NOT NULL CHECK(octet_length(manifest_sha256)=32),
 manifest jsonb NOT NULL CHECK(jsonb_typeof(manifest)='object'),
 applied_at openlegal.unix_seconds NOT NULL,
 before_state jsonb NOT NULL,
 after_state jsonb NOT NULL
);
CREATE TRIGGER immutable_current_inventory_audit
 BEFORE UPDATE OR DELETE ON openlegal.current_inventory_audit
 FOR EACH ROW EXECUTE FUNCTION openlegal.reject_snapshot_update();
