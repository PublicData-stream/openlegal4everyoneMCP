-- Immutable finite inventories and identified supplements are source observations,
-- not legal-record captures. Their bytes share permanent corpus capacity/accounting.
CREATE TABLE openlegal.corpus_source_observation (
 id text PRIMARY KEY CHECK(id ~ '^[0-9a-f]{64}$'),
 source_key text NOT NULL CHECK(octet_length(source_key) BETWEEN 1 AND 1024),
 raw_sha256 bytea CHECK(octet_length(raw_sha256)=32),
 raw_size bigint NOT NULL CHECK(raw_size BETWEEN 0 AND 16777216),
 storage_key text UNIQUE,
 media_type text NOT NULL CHECK(octet_length(media_type) BETWEEN 1 AND 128),
 rights jsonb NOT NULL CHECK(jsonb_typeof(rights)='object'),
 metadata jsonb NOT NULL CHECK(jsonb_typeof(metadata)='object'),
 observed_at openlegal.unix_seconds NOT NULL,
 validated_at openlegal.unix_seconds NOT NULL CHECK(validated_at>=observed_at),
 manifest_sha256 bytea NOT NULL CHECK(octet_length(manifest_sha256)=32),
 CHECK((raw_sha256 IS NULL AND storage_key IS NULL AND raw_size=0)
    OR (raw_sha256 IS NOT NULL AND storage_key IS NOT NULL))
);
CREATE INDEX corpus_source_observation_key ON openlegal.corpus_source_observation(source_key,observed_at,id);
