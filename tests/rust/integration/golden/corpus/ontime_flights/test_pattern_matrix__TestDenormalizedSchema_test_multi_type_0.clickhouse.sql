SELECT 
      'FLIGHT' AS "type(r)", 
      count(*) AS "cnt"
FROM default.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
HAVING count(*) > 0
