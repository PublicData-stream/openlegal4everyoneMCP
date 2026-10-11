-- Add only a local-index rejection vocabulary. Valid archived source evidence
-- and existing failed jobs retain their original state and classification.
ALTER TABLE openlegal.provider_collection_gap
    DROP CONSTRAINT provider_collection_gap_reason_check,
    ADD CONSTRAINT provider_collection_gap_reason_check
        CHECK (reason IN ('source_unavailable', 'source_data_invalid',
                         'download_failed', 'attachment_incomplete', 'index_input_limit'));

ALTER TABLE openlegal.collection_request
    DROP CONSTRAINT collection_request_reason_check,
    ADD CONSTRAINT collection_request_reason_check CHECK(reason IN (
        'ambiguous','source_inventory_incomplete','not_found','source_data_invalid',
        'source_unavailable','download_failed','identity_conflict','worker_failed','worker_lost',
        'collection_pending','already_fresh','collection_already_in_progress',
        'head_observation_superseded','publication_superseded','no_matches','multiple_skip_reasons',
        'provider_daily_limit','provider_suspended','provider_response_uncertain','provider_retry_after',
        'operation_attempt_limit','provider_admission_wait','capacity_wait','provider_recovery_hold',
        'index_input_limit'));
