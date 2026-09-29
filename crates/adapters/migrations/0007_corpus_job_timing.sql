-- Job timestamps are observations of local processing, not estimates of
-- upstream availability. Older rows remain NULL and are excluded from ETA.
ALTER TABLE openlegal.corpus_job
 ADD COLUMN started_at openlegal.unix_seconds,
 ADD COLUMN completed_at openlegal.unix_seconds;

CREATE INDEX corpus_job_completed_dataset ON openlegal.corpus_job(completed_at DESC)
 WHERE status = 'done' AND started_at IS NOT NULL AND completed_at IS NOT NULL;
