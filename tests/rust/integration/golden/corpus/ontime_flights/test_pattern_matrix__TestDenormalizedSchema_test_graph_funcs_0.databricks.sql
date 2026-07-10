SELECT 
      'FLIGHT' AS `type(r)`, 
      r.Origin AS `id(a)`, 
      array('Airport') AS `labels(a)`
FROM default.flights AS r
LIMIT 5