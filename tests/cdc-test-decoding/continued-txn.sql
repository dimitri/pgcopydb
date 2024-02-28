PREPARE c5019553 AS INSERT INTO public.readings ("time", "tags_id", "latitude", "longitude", "elevation", "velocity", "heading", "grade", "fuel_consumption", "additional_tags") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
EXECUTE c5019553["2017-07-15 02:54:30+00","48","3.46436","68.56976","115","55","30","78","1.8",null];
COMMIT; -- {"xid":6730937,"lsn":"6F/14003AD8","timestamp":"2024-02-22 19:55:32.604741+0000"}
-- SWITCH WAL {"lsn":"6F/15004368"}
