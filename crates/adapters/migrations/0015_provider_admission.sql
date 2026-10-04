-- A session lock distinguishes a live HTTP attempt from uncertain evidence.
-- Existing unresolved markers intentionally remain unowned and fail closed.
ALTER TABLE openlegal.provider_request_budget ADD COLUMN admission_owner uuid;

-- Foreground priority survives separate request Pods and is bounded by leases.
CREATE TABLE openlegal.provider_demand_ticket (
 owner uuid PRIMARY KEY,
 lease_until bigint NOT NULL
);
CREATE INDEX provider_demand_ticket_lease ON openlegal.provider_demand_ticket(lease_until);

-- Commit-delivered hints wake scanners/workers when durable backlog changes.
-- PostgreSQL coalesces equal channel/payload events within each transaction.
CREATE FUNCTION openlegal.notify_collection_work() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  PERFORM pg_notify('openlegal_collection','');
 ELSIF NEW.status IS DISTINCT FROM OLD.status
    OR NEW.lease_until IS DISTINCT FROM OLD.lease_until THEN
  PERFORM pg_notify('openlegal_collection','');
 END IF;
 RETURN NULL;
END
$$;
CREATE TRIGGER corpus_job_collection_change
 AFTER INSERT OR UPDATE OF status,lease_until ON openlegal.corpus_job
 FOR EACH ROW EXECUTE FUNCTION openlegal.notify_collection_work();
