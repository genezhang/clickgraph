SELECT 
      v1.airline AS "r.carrier", 
      v1.flight_number AS "r.flight_num", 
      v1.Dest AS "dest.code"
FROM default.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v1.Origin = 'JFK')
LIMIT 5
-- column r.carrier: Value [("r.carrier", "r.carrier")]
-- column r.flight_num: Value [("r.flight_num", "r.flight_num")]
-- column dest.code: Value [("dest.code", "dest.code")]