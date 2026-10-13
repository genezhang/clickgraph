SELECT 
      v1.OriginState AS `a.state`, 
      v1.DestCityName AS `b.city`
FROM default.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
LIMIT 10
-- column a.state: Value [("a.state", "a.state")]
-- column b.city: Value [("b.city", "b.city")]