SELECT 
      count(*) AS "bronze_count"
FROM test_integration.users_expressions_test AS v0
WHERE if((v0.score >= 1000), 'gold', if((v0.score >= 500), 'silver', 'bronze')) = 'bronze'

-- column bronze_count: Value [("bronze_count", "bronze_count")]