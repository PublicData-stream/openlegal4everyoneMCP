-- Each HTTP attempt owns a separate detached session-lock slot. Existing
-- singleton uncertain evidence remains a global fence; migration never clears it.
ALTER TABLE openlegal.provider_request_budget
 ADD COLUMN max_in_flight integer NOT NULL DEFAULT 4 CHECK(max_in_flight BETWEEN 1 AND 16),
 ADD COLUMN last_admission_mode text CHECK(last_admission_mode IN ('continuous','on_demand')),
 ALTER COLUMN continuous_daily_limit SET DEFAULT NULL,
 ALTER COLUMN interval_ms SET DEFAULT 200;

CREATE TABLE openlegal.provider_request_admission (
 owner uuid PRIMARY KEY,
 slot integer NOT NULL UNIQUE CHECK(slot BETWEEN 1 AND 16),
 mode text NOT NULL CHECK(mode IN ('continuous','on_demand','pilot')),
 started_at_ms bigint NOT NULL CHECK(started_at_ms >= 0),
 explicit_request_id uuid,
 explicit_launched_at bigint,
 CHECK ((explicit_request_id IS NULL) = (explicit_launched_at IS NULL))
);

-- Both modes register bounded waiting evidence. Expiration relinquishes fairness
-- priority; it never deletes an uncertain HTTP reservation.
ALTER TABLE openlegal.provider_demand_ticket
 ADD COLUMN mode text NOT NULL DEFAULT 'on_demand' CHECK(mode IN ('continuous','on_demand'));
