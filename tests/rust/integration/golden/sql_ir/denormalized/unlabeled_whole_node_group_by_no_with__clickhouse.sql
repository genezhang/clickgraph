SELECT 
      anyLast(t0.origin_city) AS "b.city", 
      t0.origin_code AS "b.code", 
      anyLast(t0.origin_state) AS "b.state", 
      count(*) AS "n"
FROM db_denormalized.flights_denorm AS t1
INNER JOIN db_denormalized.flights_denorm AS t0 ON t0.origin_code = t1.dest_code
WHERE (t0.flight_id <> t1.flight_id OR t0.flight_number <> t1.flight_number)
GROUP BY t0.origin_code
