-- Keep each receipt addressable while separating explicit retention from
-- automatic freshness. Retired request_key values retain canonical identity.
ALTER TABLE openlegal.collection_request
    ADD COLUMN canonical_key text,
    ADD COLUMN completed_at bigint,
    ADD COLUMN explicit_until bigint;
UPDATE openlegal.collection_request
SET canonical_key = request_key,
    explicit_until = expires_at,
    completed_at = CASE WHEN status IN ('done','skipped','failed') THEN created_at ELSE NULL END;
ALTER TABLE openlegal.collection_request ALTER COLUMN canonical_key SET NOT NULL;
CREATE INDEX collection_request_canonical ON openlegal.collection_request(canonical_key, created_at DESC);

-- Notify only after the enclosing transaction commits. The payload deliberately
-- contains no request identity or source text.
CREATE FUNCTION openlegal.notify_collection_change() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF TG_OP = 'INSERT' OR OLD.status IS DISTINCT FROM NEW.status THEN
        IF NEW.status IN ('done','skipped','failed') THEN
            NEW.completed_at := floor(extract(epoch from clock_timestamp()))::bigint;
        END IF;
        PERFORM pg_notify('openlegal_collection', '');
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER collection_change BEFORE INSERT OR UPDATE ON openlegal.collection_request
FOR EACH ROW EXECUTE FUNCTION openlegal.notify_collection_change();
