-- Keep a bounded, sanitized terminal reason after clearing request payloads.
ALTER TABLE openlegal.collection_request
 ADD COLUMN reason text CHECK (reason IN (
  'ambiguous', 'source_inventory_incomplete', 'not_found',
  'source_data_invalid', 'source_unavailable', 'download_failed',
  'identity_conflict', 'worker_failed', 'worker_lost'
 ));
