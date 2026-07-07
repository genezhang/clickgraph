SELECT 
      'FLIGHT::Airport::Airport' AS "type(r)", 
      r.Origin AS "id(a)", 
      ['Airport'] AS "labels(a)"
FROM default.flights AS r
LIMIT 5