-- Rewriting retained captures needs a separate, finite administrative budget.
-- PostgreSQL 18 does not reschedule an active positive transaction timer when
-- another positive value is assigned. Disable it only to re-arm it immediately.
SET LOCAL transaction_timeout = '0';
SET LOCAL transaction_timeout = '35min';
SET LOCAL statement_timeout = '30min';

-- Derive exactly the existing progress predicate from immutable evidence at
-- insertion/upgrade time. Status reads must not detoast every retained body.
ALTER TABLE openlegal.corpus_capture
 ADD COLUMN clone_body_complete boolean GENERATED ALWAYS AS (
  COALESCE(payload #>> '{record,metadata,attachment_status}', '') <> 'incomplete'
  AND COALESCE(payload #>> '{record,metadata,body_status}', '') <> 'response_identity_unverified_metadata_only'
 ) STORED NOT NULL;
