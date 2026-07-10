SELECT 
      r.OriginCityName AS `a.city`, 
      r.Dest AS `b.code`
FROM default.flights AS r
LIMIT 10