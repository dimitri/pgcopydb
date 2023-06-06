--
-- https://docs.timescale.com/tutorials/latest/nyc-taxi-cab/dataset-nyc/
--

BEGIN;

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE "rides"(
    vendor_id TEXT,
    pickup_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    dropoff_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    passenger_count NUMERIC,
    trip_distance NUMERIC,
    pickup_longitude  NUMERIC,
    pickup_latitude   NUMERIC,
    rate_code         INTEGER,
    dropoff_longitude NUMERIC,
    dropoff_latitude  NUMERIC,
    payment_type INTEGER,
    fare_amount NUMERIC,
    extra NUMERIC,
    mta_tax NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    improvement_surcharge NUMERIC,
    total_amount NUMERIC
);
SELECT create_hypertable('rides', 'pickup_datetime', 'payment_type', 2, create_default_indexes=>FALSE);
CREATE INDEX ON rides (vendor_id, pickup_datetime desc);
CREATE INDEX ON rides (pickup_datetime desc, vendor_id);
CREATE INDEX ON rides (rate_code, pickup_datetime DESC);
CREATE INDEX ON rides (passenger_count, pickup_datetime desc);

CREATE TABLE IF NOT EXISTS "payment_types"(
    payment_type INTEGER,
    description TEXT
);
INSERT INTO payment_types(payment_type, description) VALUES
(1, 'credit card'),
(2, 'cash'),
(3, 'no charge'),
(4, 'dispute'),
(5, 'unknown'),
(6, 'voided trip');

CREATE TABLE IF NOT EXISTS "rates"(
    rate_code   INTEGER,
    description TEXT
);
INSERT INTO rates(rate_code, description) VALUES
(1, 'standard rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'negotiated fare'),
(6, 'group ride');

COMMIT;
