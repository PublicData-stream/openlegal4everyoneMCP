-- Preserve existing nullable reasons and add explicit zero-publication outcomes.
ALTER TABLE openlegal.collection_request
 DROP CONSTRAINT collection_request_reason_check,
 ADD CONSTRAINT collection_request_reason_check CHECK (reason IN (
  'ambiguous', 'source_inventory_incomplete', 'not_found',
  'source_data_invalid', 'source_unavailable', 'download_failed',
  'identity_conflict', 'worker_failed', 'worker_lost',
  'collection_pending', 'already_fresh', 'collection_already_in_progress',
  'head_observation_superseded', 'publication_superseded',
  'no_matches', 'multiple_skip_reasons'
 ));
