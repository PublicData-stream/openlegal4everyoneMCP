-- PostgreSQL 18 only. Technical Unix seconds are supplied by the application.
CREATE SCHEMA IF NOT EXISTS openlegal;
REVOKE CREATE ON SCHEMA openlegal FROM PUBLIC;
CREATE DOMAIN openlegal.unix_seconds AS numeric(20,0)
    CHECK (VALUE >= 0 AND VALUE <= 18446744073709551615);
CREATE TABLE openlegal.cache_storage (
    id uuid PRIMARY KEY DEFAULT pg_catalog.uuidv7(),
    singleton boolean NOT NULL UNIQUE DEFAULT true CHECK (singleton),
    snapshots bigint NOT NULL DEFAULT 0 CHECK (snapshots >= 0),
    queries bigint NOT NULL DEFAULT 0 CHECK (queries >= 0),
    referenced_bytes bigint NOT NULL DEFAULT 0 CHECK (referenced_bytes >= 0)
);
INSERT INTO openlegal.cache_storage DEFAULT VALUES;
CREATE TABLE openlegal.cache_query (
    id uuid PRIMARY KEY DEFAULT pg_catalog.uuidv7(),
    namespace text NOT NULL CHECK (octet_length(namespace) BETWEEN 1 AND 128),
    provider text NOT NULL CHECK (octet_length(provider) BETWEEN 1 AND 64),
    dataset text NOT NULL CHECK (octet_length(dataset) BETWEEN 1 AND 64),
    query_hash bytea NOT NULL UNIQUE CHECK (octet_length(query_hash) = 32),
    canonical_identity bytea NOT NULL CHECK (octet_length(canonical_identity) BETWEEN 1 AND 4096),
    query jsonb NOT NULL CHECK (jsonb_typeof(query) = 'object'),
    next_sequence bigint NOT NULL DEFAULT 1 CHECK (next_sequence > 0),
    mutation_revision bigint NOT NULL DEFAULT 0 CHECK (mutation_revision >= 0),
    created_at openlegal.unix_seconds NOT NULL
);
CREATE TABLE openlegal.blob_object (
    sha256 bytea PRIMARY KEY CHECK (octet_length(sha256) = 32),
    generation uuid NOT NULL UNIQUE DEFAULT pg_catalog.uuidv7(),
    size_bytes bigint NOT NULL CHECK (size_bytes BETWEEN 0 AND 1048576),
    storage_key text NOT NULL UNIQUE CHECK (octet_length(storage_key) BETWEEN 1 AND 160),
    ready boolean NOT NULL DEFAULT false,
    created_at openlegal.unix_seconds NOT NULL,
    UNIQUE (sha256, ready)
);
CREATE TABLE openlegal.cache_snapshot (
    id uuid PRIMARY KEY DEFAULT pg_catalog.uuidv7(),
    public_id text NOT NULL UNIQUE CHECK (public_id ~ '^[0-9a-f]{64}$'),
    query_id uuid NOT NULL REFERENCES openlegal.cache_query(id),
    sequence bigint NOT NULL CHECK (sequence > 0),
    captured_at openlegal.unix_seconds NOT NULL,
    retrieved_at openlegal.unix_seconds NOT NULL,
    original_validated_at openlegal.unix_seconds NOT NULL,
    processor_version text NOT NULL CHECK (octet_length(processor_version) BETWEEN 1 AND 128),
    schema_version bigint NOT NULL CHECK (schema_version BETWEEN 1 AND 4294967295),
    raw_blob_sha256 bytea NOT NULL,
    raw_blob_ready boolean NOT NULL DEFAULT true CHECK (raw_blob_ready),
    processed_sha256 bytea NOT NULL CHECK (octet_length(processed_sha256) = 32),
    processed_data jsonb NOT NULL CHECK (jsonb_typeof(processed_data) = 'object' AND octet_length(processed_data::text) <= 131072),
    source_reference text NOT NULL CHECK (octet_length(source_reference) BETWEEN 1 AND 2048),
    envelope_sha256 bytea NOT NULL CHECK (octet_length(envelope_sha256) = 32),
    FOREIGN KEY (raw_blob_sha256, raw_blob_ready) REFERENCES openlegal.blob_object(sha256, ready),
    UNIQUE (query_id, sequence),
    UNIQUE (id, query_id, processor_version, schema_version)
);
CREATE INDEX snapshot_retention ON openlegal.cache_snapshot(captured_at, id);
CREATE INDEX snapshot_blob ON openlegal.cache_snapshot(raw_blob_sha256);
CREATE TABLE openlegal.cache_head (
    query_id uuid NOT NULL,
    processor_version text NOT NULL,
    schema_version bigint NOT NULL,
    snapshot_id uuid NOT NULL,
    validated_at openlegal.unix_seconds NOT NULL,
    etag text CHECK (octet_length(etag) <= 2048),
    last_modified text CHECK (octet_length(last_modified) <= 2048),
    PRIMARY KEY (query_id, processor_version, schema_version),
    FOREIGN KEY (snapshot_id, query_id, processor_version, schema_version)
      REFERENCES openlegal.cache_snapshot(id, query_id, processor_version, schema_version)
);
CREATE TABLE openlegal.blob_deletion (
    id uuid PRIMARY KEY DEFAULT pg_catalog.uuidv7(),
    storage_key text NOT NULL UNIQUE CHECK (octet_length(storage_key) BETWEEN 1 AND 160),
    sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
    generation uuid NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes BETWEEN 0 AND 1048576),
    queued_at openlegal.unix_seconds NOT NULL
);
-- Cache observations are append-only. Retention explicitly deletes whole occurrences.
CREATE FUNCTION openlegal.reject_snapshot_update() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog AS $$ BEGIN
    RAISE EXCEPTION 'immutable captured occurrence' USING ERRCODE = '23514';
END $$;
CREATE TRIGGER immutable_snapshot BEFORE UPDATE ON openlegal.cache_snapshot
FOR EACH ROW EXECUTE FUNCTION openlegal.reject_snapshot_update();
