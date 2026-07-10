SELECT 
      r1.OriginCityName AS `a.city`, 
      r2.DestCityName AS `c.city`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Origin = r1.Dest
LIMIT 5