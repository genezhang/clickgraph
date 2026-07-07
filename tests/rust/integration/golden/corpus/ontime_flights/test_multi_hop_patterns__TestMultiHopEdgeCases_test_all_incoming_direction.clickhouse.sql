SELECT 
      r1.Dest AS "a.code", 
      r2.Origin AS "c.code"
FROM default.flights AS r1
JOIN default.flights AS r2 ON 1 = 1
LIMIT 5