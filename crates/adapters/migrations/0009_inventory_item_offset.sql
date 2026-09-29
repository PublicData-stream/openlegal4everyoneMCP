-- Preserve progress within a list page while each dataset has a bounded
-- share of the detail queue. The offset is advisory: replay is idempotent.
ALTER TABLE openlegal.provider_inventory_cursor
 ADD COLUMN item_offset integer NOT NULL DEFAULT 0 CHECK (item_offset BETWEEN 0 AND 100);

CREATE INDEX corpus_outbox_head_capture_lookup
 ON openlegal.corpus_outbox (object_key,capture_id,sequence DESC)
 WHERE is_head AND NOT removed;
