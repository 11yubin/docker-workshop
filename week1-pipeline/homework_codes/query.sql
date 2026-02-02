-- Q3
SELECT COUNT(*)
FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;

-- Q4
SELECT DATE(lpep_pickup_datetime) AS pickup_day, MAX(trip_distance)
FROM green_taxi_data
WHERE trip_distance < 100
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

-- Q5
SELECT z."Zone", SUM(t.total_amount) AS total
FROM green_taxi_data t
JOIN zones z ON t."PULocationID" = z."LocationID"
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total DESC
LIMIT 1;

-- Q6
SELECT zdo."Zone", MAX(t.tip_amount) as max_tip
FROM green_taxi_data t
JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime < '2025-12-01'
GROUP BY zdo."Zone"
ORDER BY max_tip DESC
LIMIT 1;