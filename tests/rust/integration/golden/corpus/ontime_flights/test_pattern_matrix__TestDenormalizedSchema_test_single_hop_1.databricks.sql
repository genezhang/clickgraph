SELECT 
      r.OriginCityName AS `a.city`, 
      r.DestCityName AS `b.city`
FROM default.flights AS r
LIMIT 10