-- One durable admission point for all LAW OPEN DATA calls, including attachments.
-- A reserved attempt counts even when DNS, TLS or the response later fails.
CREATE TABLE openlegal.provider_request_budget (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 utc_day bigint NOT NULL DEFAULT -1,
 daily_used integer NOT NULL DEFAULT 0 CHECK(daily_used BETWEEN 0 AND 1000),
 next_allowed_at bigint NOT NULL DEFAULT 0,
 operator_suspended boolean NOT NULL DEFAULT false,
 unresolved_response boolean NOT NULL DEFAULT false,
 pilot_started_at bigint,
 pilot_used integer NOT NULL DEFAULT 0 CHECK(pilot_used BETWEEN 0 AND 100)
);
INSERT INTO openlegal.provider_request_budget DEFAULT VALUES;

-- Historical detail bytes may be corrected under the same provider revision.
-- This mutable stamp records a successful unchanged revalidation without
-- modifying immutable capture evidence or emitting another index event.
ALTER TABLE openlegal.corpus_revision ADD COLUMN last_validated_at openlegal.unix_seconds;
UPDATE openlegal.corpus_revision r SET last_validated_at=c.captured_at
 FROM openlegal.corpus_capture c WHERE c.id=r.latest_capture;

-- Incremental collection resumes after a server restart. These cursors do
-- not imply a stable or complete provider inventory.
CREATE TABLE openlegal.provider_inventory_cursor (
 dataset text PRIMARY KEY,
 next_page integer NOT NULL DEFAULT 1 CHECK(next_page BETWEEN 1 AND 1000000)
);
