SELECT 
      count(*) AS "count(*)"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest = v1.Origin)

-- column count(*): Value [("count(*)", "count(*)")]