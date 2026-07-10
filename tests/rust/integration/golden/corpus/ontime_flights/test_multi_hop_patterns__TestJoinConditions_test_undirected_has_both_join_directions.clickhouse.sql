SELECT `a.code` AS `a.code` FROM (
SELECT 
      r1.Origin AS "a.code"
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
WHERE NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number)
UNION ALL 
SELECT 
      r1.Dest AS "a.code"
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Origin
WHERE NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number)
UNION ALL 
SELECT 
      r1.Origin AS "a.code"
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Dest
WHERE NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number)
UNION ALL 
SELECT 
      r1.Dest AS "a.code"
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Origin
WHERE NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number)
) AS __union
LIMIT 1