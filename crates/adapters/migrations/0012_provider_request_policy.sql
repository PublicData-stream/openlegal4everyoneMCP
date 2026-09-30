-- Operator-selected limits are durable and shared by all provider clients.
-- Preserve charged attempts, pauses, suspension and unresolved-response evidence.
ALTER TABLE openlegal.provider_request_budget
 DROP CONSTRAINT provider_request_budget_daily_used_check,
 DROP CONSTRAINT provider_request_budget_on_demand_used_check,
 ADD CONSTRAINT provider_request_budget_daily_used_check CHECK(daily_used BETWEEN 0 AND 1000000),
 ADD CONSTRAINT provider_request_budget_on_demand_used_check CHECK(on_demand_used BETWEEN 0 AND 1000000),
 ADD COLUMN continuous_daily_limit integer NOT NULL DEFAULT 1000 CHECK(continuous_daily_limit BETWEEN 1 AND 1000000),
 ADD COLUMN on_demand_daily_limit integer NOT NULL DEFAULT 1000 CHECK(on_demand_daily_limit BETWEEN 1 AND 1000000),
 ADD COLUMN min_interval_secs integer NOT NULL DEFAULT 5 CHECK(min_interval_secs BETWEEN 1 AND 3600),
 ADD COLUMN next_request_at_ms bigint NOT NULL DEFAULT 0 CHECK(next_request_at_ms >= 0);

-- Older releases also stored request spacing in this pause timestamp.
UPDATE openlegal.provider_request_budget
 SET next_request_at_ms=LEAST(next_allowed_at,9223372036854775)*1000;
