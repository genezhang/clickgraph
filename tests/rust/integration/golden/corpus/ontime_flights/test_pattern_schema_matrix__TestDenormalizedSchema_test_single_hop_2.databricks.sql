SELECT 
      v1.OriginCityName AS `a.city`, 
      v1.Dest AS `b.code`
FROM default.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
LIMIT 10