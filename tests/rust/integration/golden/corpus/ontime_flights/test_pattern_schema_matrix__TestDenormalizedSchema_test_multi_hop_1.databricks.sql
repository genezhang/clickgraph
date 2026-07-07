SELECT 
      r1.airport AS `a.airport`, 
      r2.airport AS `c.airport`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
LIMIT 5