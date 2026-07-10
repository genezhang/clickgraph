SELECT 
      r1.Origin AS "a.code"
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
LIMIT 1