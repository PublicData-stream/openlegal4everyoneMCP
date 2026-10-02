-- Shared citation retention is independent of cursor/generation sessions. A
-- capture can have only one renewable lease, bounded by application admission.
CREATE TABLE openlegal.corpus_citation_lease (
 capture_id text PRIMARY KEY REFERENCES openlegal.corpus_capture(id) ON DELETE CASCADE,
 expires_at openlegal.unix_seconds NOT NULL
);
CREATE INDEX corpus_citation_lease_expiry ON openlegal.corpus_citation_lease(expires_at);
