-- Evidence is a permanent archive. NULL capacity means no configured total cap;
-- temporary staging and per-response bounds are independent of this policy.
ALTER TABLE openlegal.corpus_control
 ADD COLUMN permanent_archive boolean NOT NULL DEFAULT true CHECK(permanent_archive),
 ADD COLUMN max_raw_bytes numeric(20,0) CHECK(max_raw_bytes>0 AND max_raw_bytes<=18446744073709551615);

-- Migration and corpus maintenance/publication serialize on the control row.
-- Restore indexing for retired captures whose database evidence still exists.
-- Missing, physically deleted bytes cannot be reconstructed by this migration;
-- the surviving catalog and prior retirement proof must remain unchanged.
DO $$
DECLARE
 retained record;
 event bigint;
BEGIN
 PERFORM singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE;
 FOR retained IN
  SELECT c.id,c.object_key,o.version,o.withdrawn,COALESCE(o.head_capture=c.id,false) AS is_head
  FROM openlegal.corpus_retirement t
  JOIN openlegal.corpus_capture c ON c.id=t.capture_id
  JOIN openlegal.corpus_object o USING(object_key)
  ORDER BY t.event_sequence,c.id
 LOOP
  UPDATE openlegal.corpus_control SET next_event=next_event+1 RETURNING next_event-1 INTO event;
  INSERT INTO openlegal.corpus_outbox(sequence,object_key,object_version,capture_id,withdrawn,is_head,removed)
   VALUES(event,retained.object_key,retained.version,retained.id,retained.withdrawn,retained.is_head,false);
 END LOOP;
 DELETE FROM openlegal.corpus_retirement t WHERE EXISTS(SELECT 1 FROM openlegal.corpus_capture c WHERE c.id=t.capture_id);
 DELETE FROM openlegal.corpus_blob_deletion d
  WHERE EXISTS(SELECT 1 FROM openlegal.corpus_capture c WHERE c.storage_key=d.storage_key)
     OR EXISTS(SELECT 1 FROM openlegal.corpus_capture_blob b WHERE b.storage_key=d.storage_key);
END $$;
