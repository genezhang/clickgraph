SELECT 
      r.airport AS `a.airport`, 
      r.DestState AS `b.state`
FROM default.flights AS r
LIMIT 10