SELECT 
      count(*) AS "total"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)

-- column total: Value [("total", "total")]