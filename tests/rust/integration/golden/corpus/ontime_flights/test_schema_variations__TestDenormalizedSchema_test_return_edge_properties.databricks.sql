SELECT 
      r.airline AS `r.carrier`, 
      r.flight_number AS `r.flight_num`, 
      r.Dest AS `dest.code`
FROM default.flights AS r
WHERE r.Origin = 'JFK'
LIMIT 5