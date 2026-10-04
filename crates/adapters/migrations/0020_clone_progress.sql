-- Traversal progress is independent of read/citation freshness. Two matching
-- complete observations are evidence of an observed clone, not an atomic
-- upstream snapshot or permission to enable exact-date completeness selectors.
CREATE TABLE openlegal.provider_clone_view (
 view_key text PRIMARY KEY,
 dataset text NOT NULL,
 historical boolean NOT NULL,
 treaty_class smallint CHECK(treaty_class IN (1,2)),
 next_page integer NOT NULL DEFAULT 1 CHECK(next_page>0),
 item_offset integer NOT NULL DEFAULT 0 CHECK(item_offset>=0),
 page_digest bytea,
 cycle bigint NOT NULL DEFAULT 1 CHECK(cycle>0),
 expected_total bigint CHECK(expected_total>=0),
 cycle_invalid boolean NOT NULL DEFAULT false,
 last_digest bytea,
 stable_cycles integer NOT NULL DEFAULT 0 CHECK(stable_cycles>=0),
 last_completed_at bigint,
 last_observed_at bigint
);
CREATE TABLE openlegal.provider_clone_member (
 view_key text NOT NULL REFERENCES openlegal.provider_clone_view(view_key),
 object_key text NOT NULL,
 revision_id text NOT NULL,
 seen_cycle bigint NOT NULL,
 required_body boolean NOT NULL,
 PRIMARY KEY(view_key,object_key,revision_id)
);
CREATE INDEX provider_clone_member_cycle ON openlegal.provider_clone_member(view_key,seen_cycle);
