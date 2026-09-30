-- Revision chronology is distinct from local capture observation sequence.
-- Unknown dates sort last; provider revision IDs break ties bytewise.
CREATE INDEX corpus_revision_history_order
 ON openlegal.corpus_revision
 (object_key, (COALESCE(effective_date,publication_date,'')) DESC, revision_id COLLATE "C" ASC);
