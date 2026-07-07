SELECT 
      r.OriginState AS "a.state", 
      r.DestCityName AS "b.city"
FROM default.flights AS r
LIMIT 10