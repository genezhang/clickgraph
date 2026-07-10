SELECT `a.code` AS `a.code` FROM (
SELECT 
      r1.Origin AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
INNER JOIN default.flights AS r3 ON r3.Origin = r2.Dest
INNER JOIN default.flights AS r4 ON r4.Origin = r3.Dest
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Dest AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Origin
INNER JOIN default.flights AS r3 ON r3.Origin = r2.Dest
INNER JOIN default.flights AS r4 ON r4.Origin = r3.Dest
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Origin AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Dest
INNER JOIN default.flights AS r3 ON r3.Origin = r2.Origin
INNER JOIN default.flights AS r4 ON r4.Origin = r3.Dest
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Dest AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Origin
INNER JOIN default.flights AS r3 ON r3.Origin = r2.Origin
INNER JOIN default.flights AS r4 ON r4.Origin = r3.Dest
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Origin AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
INNER JOIN default.flights AS r3 ON r3.Dest = r2.Dest
INNER JOIN default.flights AS r4 ON r4.Origin = r3.Origin
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Dest AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Origin
INNER JOIN default.flights AS r3 ON r3.Dest = r2.Dest
INNER JOIN default.flights AS r4 ON r4.Origin = r3.Origin
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Origin AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Dest
INNER JOIN default.flights AS r3 ON r3.Dest = r2.Origin
INNER JOIN default.flights AS r4 ON r4.Origin = r3.Origin
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Dest AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Origin
INNER JOIN default.flights AS r3 ON r3.Dest = r2.Origin
INNER JOIN default.flights AS r4 ON r4.Origin = r3.Origin
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Origin AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
INNER JOIN default.flights AS r3 ON r3.Origin = r2.Dest
INNER JOIN default.flights AS r4 ON r4.Dest = r3.Dest
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Dest AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Origin
INNER JOIN default.flights AS r3 ON r3.Origin = r2.Dest
INNER JOIN default.flights AS r4 ON r4.Dest = r3.Dest
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Origin AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Dest
INNER JOIN default.flights AS r3 ON r3.Origin = r2.Origin
INNER JOIN default.flights AS r4 ON r4.Dest = r3.Dest
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Dest AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Origin
INNER JOIN default.flights AS r3 ON r3.Origin = r2.Origin
INNER JOIN default.flights AS r4 ON r4.Dest = r3.Dest
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Origin AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
INNER JOIN default.flights AS r3 ON r3.Dest = r2.Dest
INNER JOIN default.flights AS r4 ON r4.Dest = r3.Origin
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Dest AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Origin
INNER JOIN default.flights AS r3 ON r3.Dest = r2.Dest
INNER JOIN default.flights AS r4 ON r4.Dest = r3.Origin
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Origin AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Dest
INNER JOIN default.flights AS r3 ON r3.Dest = r2.Origin
INNER JOIN default.flights AS r4 ON r4.Dest = r3.Origin
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
UNION ALL 
SELECT 
      r1.Dest AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Origin
INNER JOIN default.flights AS r3 ON r3.Dest = r2.Origin
INNER JOIN default.flights AS r4 ON r4.Dest = r3.Origin
WHERE (((((NOT (r4.flight_id = r3.flight_id AND r4.flight_number = r3.flight_number) AND NOT (r4.flight_id = r2.flight_id AND r4.flight_number = r2.flight_number)) AND NOT (r4.flight_id = r1.flight_id AND r4.flight_number = r1.flight_number)) AND NOT (r3.flight_id = r2.flight_id AND r3.flight_number = r2.flight_number)) AND NOT (r3.flight_id = r1.flight_id AND r3.flight_number = r1.flight_number)) AND NOT (r2.flight_id = r1.flight_id AND r2.flight_number = r1.flight_number))
) AS __union
LIMIT 1