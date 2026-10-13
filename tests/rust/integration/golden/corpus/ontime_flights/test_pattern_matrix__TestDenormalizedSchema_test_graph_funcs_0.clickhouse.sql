SELECT 
      'FLIGHT' AS "type(r)", 
      v1.Origin AS "id(a)", 
      ['Airport'] AS "labels(a)"
FROM default.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
LIMIT 5