-- Keep claim and bounded temporary cleanup work independent of archive size.
CREATE INDEX corpus_job_ready_order ON openlegal.corpus_job(created_at,id)
 WHERE status IN ('pending','running');
CREATE INDEX corpus_job_active_object ON openlegal.corpus_job(object_key,lease_until,id)
 WHERE status='running';
CREATE INDEX corpus_staging_created ON openlegal.corpus_staging(created_at,storage_key);
CREATE INDEX corpus_session_expiry ON openlegal.corpus_session(expires_at,id);
CREATE INDEX corpus_session_invalidated ON openlegal.corpus_session(id) WHERE invalidated;
