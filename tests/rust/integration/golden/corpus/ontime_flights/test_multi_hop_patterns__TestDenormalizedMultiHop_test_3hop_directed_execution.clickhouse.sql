SELECT 
      f1.Origin AS "a.code", 
      f2.Origin AS "b.code", 
      f3.Origin AS "c.code", 
      f3.Dest AS "d.code"
FROM default.flights AS f1
INNER JOIN default.flights AS f2 ON f2.Origin = f1.Dest
INNER JOIN default.flights AS f3 ON f3.Origin = f2.Dest
LIMIT 5