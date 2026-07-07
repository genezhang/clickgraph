SELECT 
      r1.OriginState AS `a.state`, 
      r2.DestState AS `c.state`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
LIMIT 5