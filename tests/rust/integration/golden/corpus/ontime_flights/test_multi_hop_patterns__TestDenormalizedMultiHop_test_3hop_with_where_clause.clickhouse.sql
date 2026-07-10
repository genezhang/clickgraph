SELECT 
      f1.Origin AS "a.code", 
      f2.Origin AS "b.code", 
      f3.Origin AS "c.code", 
      f3.Dest AS "d.code", 
      f1.airline AS "f1.carrier"
FROM default.flights AS f1
INNER JOIN default.flights AS f2 ON f2.Origin = f1.Dest
INNER JOIN default.flights AS f3 ON f3.Origin = f2.Dest
WHERE (f1.airline = f2.airline AND f2.airline = f3.airline)
LIMIT 5