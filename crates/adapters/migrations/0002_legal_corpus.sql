-- Durable legal objects are independent of disposable retrieval query caches.
CREATE TABLE openlegal.corpus_control (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), next_event bigint NOT NULL DEFAULT 1 CHECK(next_event>0),
 index_ack bigint NOT NULL DEFAULT 0 CHECK(index_ack>=0), historical_bytes bigint NOT NULL DEFAULT 0 CHECK(historical_bytes>=0), raw_bytes bigint NOT NULL DEFAULT 0 CHECK(raw_bytes>=0), staged_bytes bigint NOT NULL DEFAULT 0 CHECK(staged_bytes>=0)
);
INSERT INTO openlegal.corpus_control DEFAULT VALUES;
CREATE TABLE openlegal.corpus_object (
 object_key text PRIMARY KEY CHECK(object_key ~ '^[0-9a-f]{64}$'), identity jsonb NOT NULL,
 version bigint NOT NULL DEFAULT 0 CHECK(version>=0), catalog_version bigint NOT NULL DEFAULT 0 CHECK(catalog_version>=0), next_capture bigint NOT NULL DEFAULT 1 CHECK(next_capture>0),
 desired_head_revision text, head_capture text, validated_at openlegal.unix_seconds, pending boolean NOT NULL DEFAULT false,
 withdrawn boolean NOT NULL DEFAULT false, inventory_complete boolean NOT NULL DEFAULT false
);
CREATE TABLE openlegal.corpus_capture (
 id text PRIMARY KEY CHECK(id ~ '^[0-9a-f]{64}$'), object_key text NOT NULL REFERENCES openlegal.corpus_object,
 revision_id text NOT NULL CHECK(octet_length(revision_id) BETWEEN 1 AND 256), sequence bigint NOT NULL CHECK(sequence>0),
 captured_at openlegal.unix_seconds NOT NULL, event_sequence bigint NOT NULL, publication_date text, effective_date text,
 payload jsonb NOT NULL, payload_sha256 bytea NOT NULL CHECK(octet_length(payload_sha256)=32),
 raw_sha256 bytea NOT NULL CHECK(octet_length(raw_sha256)=32), raw_size bigint NOT NULL CHECK(raw_size BETWEEN 0 AND 104857600),
 storage_key text NOT NULL UNIQUE, expires_at openlegal.unix_seconds,
 UNIQUE(object_key,sequence), UNIQUE(object_key,id)
);
CREATE TRIGGER immutable_corpus_capture BEFORE UPDATE ON openlegal.corpus_capture
 FOR EACH ROW EXECUTE FUNCTION openlegal.reject_snapshot_update();
ALTER TABLE openlegal.corpus_object ADD FOREIGN KEY(object_key,head_capture) REFERENCES openlegal.corpus_capture(object_key,id);
CREATE INDEX corpus_capture_revision ON openlegal.corpus_capture(object_key,revision_id,sequence DESC);
CREATE TABLE openlegal.corpus_revision (
 object_key text NOT NULL REFERENCES openlegal.corpus_object, revision_id text NOT NULL,
 latest_capture text, publication_date text, effective_date text, last_sequence bigint NOT NULL, captured_at openlegal.unix_seconds,
 PRIMARY KEY(object_key,revision_id), FOREIGN KEY(object_key,latest_capture) REFERENCES openlegal.corpus_capture(object_key,id)
);
CREATE TABLE openlegal.corpus_job (
 id uuid PRIMARY KEY DEFAULT pg_catalog.uuidv7(), object_key text NOT NULL REFERENCES openlegal.corpus_object,
 source_metadata jsonb NOT NULL DEFAULT '{}', revision_id text NOT NULL, effective_date text NOT NULL DEFAULT '', install_head boolean NOT NULL DEFAULT true, expected_version bigint NOT NULL, status text NOT NULL CHECK(status IN ('pending','running','done','failed')),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 3), created_at openlegal.unix_seconds NOT NULL,
 lease_until openlegal.unix_seconds, error_category text,
 UNIQUE(object_key,revision_id,effective_date)
);
CREATE TABLE openlegal.corpus_outbox (
 sequence bigint PRIMARY KEY, object_key text NOT NULL REFERENCES openlegal.corpus_object,
 object_version bigint NOT NULL, capture_id text, withdrawn boolean NOT NULL, is_head boolean NOT NULL, removed boolean NOT NULL DEFAULT false
);
CREATE TABLE openlegal.corpus_session (
 id text PRIMARY KEY CHECK(id ~ '^[0-9a-f]{64}$'), generation bigint NOT NULL CHECK(generation>=0),
 expires_at openlegal.unix_seconds NOT NULL, invalidated boolean NOT NULL DEFAULT false
);
CREATE TABLE openlegal.corpus_pin (
 session_id text NOT NULL REFERENCES openlegal.corpus_session ON DELETE CASCADE,
 capture_id text NOT NULL REFERENCES openlegal.corpus_capture, PRIMARY KEY(session_id,capture_id)
);
CREATE TABLE openlegal.corpus_blob_deletion (
 storage_key text PRIMARY KEY, raw_sha256 bytea NOT NULL, raw_size bigint NOT NULL
);
CREATE TABLE openlegal.corpus_staging (
 storage_key text PRIMARY KEY, raw_sha256 bytea NOT NULL, raw_size bigint NOT NULL, created_at openlegal.unix_seconds NOT NULL
);

CREATE TABLE openlegal.corpus_retirement (capture_id text PRIMARY KEY REFERENCES openlegal.corpus_capture,event_sequence bigint NOT NULL);

CREATE TABLE openlegal.corpus_capture_blob (
 capture_id text NOT NULL REFERENCES openlegal.corpus_capture ON DELETE CASCADE,
 ordinal integer NOT NULL, raw_sha256 bytea NOT NULL CHECK(octet_length(raw_sha256)=32),
 raw_size bigint NOT NULL CHECK(raw_size BETWEEN 0 AND 104857600), storage_key text NOT NULL UNIQUE,
 PRIMARY KEY(capture_id,ordinal)
);
CREATE TRIGGER immutable_corpus_capture_blob BEFORE UPDATE ON openlegal.corpus_capture_blob
 FOR EACH ROW EXECUTE FUNCTION openlegal.reject_snapshot_update();

CREATE TABLE openlegal.corpus_capture_catalog (
 id text PRIMARY KEY, object_key text NOT NULL REFERENCES openlegal.corpus_object, revision_id text NOT NULL,
 sequence bigint NOT NULL, captured_at openlegal.unix_seconds NOT NULL,
 publication_date text, effective_date text, payload jsonb NOT NULL, payload_sha256 bytea NOT NULL CHECK(octet_length(payload_sha256)=32),
 UNIQUE(object_key,sequence)
);
CREATE INDEX corpus_capture_catalog_revision ON openlegal.corpus_capture_catalog(object_key,revision_id,sequence DESC);
CREATE TRIGGER immutable_corpus_capture_catalog BEFORE UPDATE ON openlegal.corpus_capture_catalog
 FOR EACH ROW EXECUTE FUNCTION openlegal.reject_snapshot_update();
