SELECT 
      r1.Origin AS `a.code`, 
      r2.Dest AS `b.code`, 
      r2.Origin AS `c.code`
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.Dest = r1.Dest
WHERE (r2.flight_id <> r1.flight_id OR r2.flight_number <> r1.flight_number)
LIMIT 5