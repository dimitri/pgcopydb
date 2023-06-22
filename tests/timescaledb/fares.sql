--
-- https://docs.timescale.com/tutorials/latest/nyc-taxi-cab/query-nyc/
--

SELECT rate_code, COUNT(vendor_id) AS num_trips
FROM rides
WHERE pickup_datetime < '2016-01-08'
GROUP BY rate_code
ORDER BY rate_code;
