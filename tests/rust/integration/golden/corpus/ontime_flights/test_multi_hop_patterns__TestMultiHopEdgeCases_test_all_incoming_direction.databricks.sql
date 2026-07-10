SELECT 
      r1.Dest AS `a.code`, 
      r2.Origin AS `c.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Origin
LIMIT 5