----------------------------------------
-- Hypertable to store signalk metrics
----------------------------------------
-- Step 1: Define regular table
CREATE TABLE IF NOT EXISTS sensor_metrics (

   time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
   device_id text NOT NULL,
   path text NOT NULL,
   value DOUBLE PRECISION NULL
);

-- Step 2: Turn into hypertable
SELECT create_hypertable('sensor_metrics','time');
