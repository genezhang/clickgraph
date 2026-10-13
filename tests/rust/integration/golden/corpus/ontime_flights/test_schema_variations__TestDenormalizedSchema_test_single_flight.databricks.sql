SELECT 
      v1.Dest AS `dest.code`, 
      v1.DestCityName AS `dest.city`
FROM default.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v1.Origin = 'JFK')
LIMIT 10