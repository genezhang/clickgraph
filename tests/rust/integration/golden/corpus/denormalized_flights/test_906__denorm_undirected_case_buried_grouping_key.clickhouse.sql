SELECT CASE WHEN count(`r.flight_id`) > 5 THEN `r.Origin` ELSE 'x' END AS "m" FROM (
SELECT 
      r.Origin AS "r.Origin",
      r.flight_id AS "r.flight_id"
FROM test_integration.flights AS r
UNION ALL 
SELECT 
      r.Dest AS "r.Origin",
      r.flight_id AS "r.flight_id"
FROM test_integration.flights AS r
) AS __union
GROUP BY `r.Origin`
