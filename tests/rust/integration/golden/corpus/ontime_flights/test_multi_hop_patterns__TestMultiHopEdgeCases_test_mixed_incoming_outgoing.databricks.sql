SELECT 
      r1.Origin AS `a.code`, 
      r2.Dest AS `b.code`, 
      r2.Origin AS `c.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Dest
LIMIT 5