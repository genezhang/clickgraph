SELECT 
      'FLIGHT::Airport::Airport' AS "type(r)", 
      count(*) AS "cnt"
FROM default.flights AS r
GROUP BY 'FLIGHT::Airport::Airport'
