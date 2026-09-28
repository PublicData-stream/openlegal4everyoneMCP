-- Provider observations that could not be published. These are operational
-- gaps, not legal records or proof of a complete upstream inventory.
CREATE TABLE openlegal.provider_collection_gap (
 gap_key text PRIMARY KEY CHECK(length(gap_key) BETWEEN 1 AND 256),
 dataset text NOT NULL CHECK(length(dataset) BETWEEN 1 AND 64),
 scope text NOT NULL CHECK(scope IN ('page','detail')),
 historical boolean NOT NULL DEFAULT false,
 treaty_class smallint,
 page integer,
 object_key text,
 revision_id text,
 reason text NOT NULL CHECK(reason IN ('source_unavailable','source_data_invalid','attachment_incomplete')),
 affected_rows integer NOT NULL DEFAULT 1 CHECK(affected_rows BETWEEN 1 AND 100),
 first_seen_at bigint NOT NULL,
 last_seen_at bigint NOT NULL,
 retry_at bigint NOT NULL,
 resolved_at bigint,
 CHECK((scope='page' AND page IS NOT NULL AND object_key IS NULL) OR
       (scope='detail' AND page IS NULL AND object_key IS NOT NULL AND revision_id IS NOT NULL))
);
CREATE INDEX provider_collection_gap_retry ON openlegal.provider_collection_gap(retry_at, dataset)
 WHERE resolved_at IS NULL;
CREATE INDEX provider_collection_gap_object ON openlegal.provider_collection_gap(object_key)
 WHERE resolved_at IS NULL AND object_key IS NOT NULL;
