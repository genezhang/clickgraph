SELECT 
      r.Origin AS `a.code`, 
      r.Dest AS `b.code`
FROM default.flights AS r
LIMIT 10