-- Distinguish transient download failures from confirmed 404/410 absence and
-- invalid downloaded bytes. Existing rows retain their original meaning.
ALTER TABLE openlegal.provider_collection_gap
    DROP CONSTRAINT provider_collection_gap_reason_check,
    ADD CONSTRAINT provider_collection_gap_reason_check
        CHECK (reason IN ('source_unavailable', 'source_data_invalid',
                         'download_failed', 'attachment_incomplete'));
