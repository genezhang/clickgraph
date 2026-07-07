SELECT 
      t0.Dest AS "dest.code", 
      t0.DestCityName AS "dest.city"
FROM default.flights AS t0
WHERE t0.Origin = 'JFK'
LIMIT 10