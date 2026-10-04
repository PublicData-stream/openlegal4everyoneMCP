-- Durable closed-source work descriptors. Large seed populations remain on disk;
-- claim execution materializes one descriptor and never a whole source inventory.
CREATE TABLE openlegal.provider_supplement_control (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 last_global_source text NOT NULL DEFAULT '',
 last_seed_source text NOT NULL DEFAULT '',
 last_global boolean NOT NULL DEFAULT false
);
INSERT INTO openlegal.provider_supplement_control DEFAULT VALUES;
CREATE TABLE openlegal.provider_supplement_job (
 job_key text PRIMARY KEY CHECK(octet_length(job_key) BETWEEN 1 AND 1024),
 source text NOT NULL CHECK(octet_length(source) BETWEEN 1 AND 128),
 descriptor jsonb NOT NULL CHECK(jsonb_typeof(descriptor)='array' AND jsonb_array_length(descriptor)=3 AND octet_length(descriptor::text)<=4096),
 page integer NOT NULL CHECK(page BETWEEN 1 AND 1000000),
 predecessor_key text,
 is_global boolean NOT NULL,
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','done','deferred','incomplete')),
 owner uuid,
 lease_until openlegal.unix_seconds,
 observation_id text REFERENCES openlegal.corpus_source_observation(id),
 observed_before openlegal.unix_seconds NOT NULL DEFAULT 0,
 observed_rows openlegal.unix_seconds NOT NULL DEFAULT 0,
 created_at openlegal.unix_seconds NOT NULL,
 requested_at openlegal.unix_seconds NOT NULL,
 completed_at openlegal.unix_seconds,
 retry_at openlegal.unix_seconds,
 CHECK((status='running')=(owner IS NOT NULL)),
 CHECK((status='running')=(lease_until IS NOT NULL)),
 CHECK((page=1)=(predecessor_key IS NULL)),
 CHECK(status NOT IN ('done','deferred') OR completed_at IS NOT NULL),
 CHECK(status<>'done' OR observation_id IS NOT NULL)
);
CREATE INDEX provider_supplement_pending ON openlegal.provider_supplement_job(is_global,source,created_at,job_key) WHERE status='pending';
CREATE INDEX provider_supplement_recovery ON openlegal.provider_supplement_job(status,retry_at,lease_until) WHERE status IN ('running','incomplete');
CREATE INDEX provider_supplement_source_status ON openlegal.provider_supplement_job(source,status);
CREATE FUNCTION openlegal.reject_supplement_descriptor_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.job_key IS DISTINCT FROM OLD.job_key OR NEW.source IS DISTINCT FROM OLD.source
    OR NEW.descriptor IS DISTINCT FROM OLD.descriptor OR NEW.page IS DISTINCT FROM OLD.page
    OR NEW.predecessor_key IS DISTINCT FROM OLD.predecessor_key OR NEW.is_global IS DISTINCT FROM OLD.is_global
    OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
  RAISE EXCEPTION 'supplement work descriptor is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_supplement_descriptor BEFORE UPDATE ON openlegal.provider_supplement_job
 FOR EACH ROW EXECUTE FUNCTION openlegal.reject_supplement_descriptor_update();
