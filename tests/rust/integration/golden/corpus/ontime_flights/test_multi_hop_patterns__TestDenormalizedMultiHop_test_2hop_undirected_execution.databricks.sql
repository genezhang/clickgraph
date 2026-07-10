SELECT `a.code` AS `a.code`, `b.code` AS `b.code`, `c.code` AS `c.code` FROM (
SELECT 
      r1.Origin AS `a.code`, 
      r2.Origin AS `b.code`, 
      r2.Dest AS `c.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
WHERE NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number)
UNION ALL 
SELECT 
      r1.Dest AS `a.code`, 
      r2.Origin AS `b.code`, 
      r2.Dest AS `c.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Origin
WHERE NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number)
UNION ALL 
SELECT 
      r1.Origin AS `a.code`, 
      r2.Dest AS `b.code`, 
      r2.Origin AS `c.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Dest
WHERE NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number)
UNION ALL 
SELECT 
      r1.Dest AS `a.code`, 
      r2.Dest AS `b.code`, 
      r2.Origin AS `c.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Origin
WHERE NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number)
) AS __union
LIMIT 10