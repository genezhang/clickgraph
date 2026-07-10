SELECT 
      'FLIGHT' AS "type(r)", 
      count(*) AS "cnt"
FROM default.flights AS r
GROUP BY 'FLIGHT'
