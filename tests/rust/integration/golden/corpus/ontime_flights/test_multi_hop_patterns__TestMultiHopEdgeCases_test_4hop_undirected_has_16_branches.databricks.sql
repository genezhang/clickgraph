SELECT 
      r1.Origin AS `a.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
INNER JOIN default.flights AS r3 ON r3.Origin = r2.Dest
INNER JOIN default.flights AS r4 ON r4.Origin = r3.Dest
LIMIT 1