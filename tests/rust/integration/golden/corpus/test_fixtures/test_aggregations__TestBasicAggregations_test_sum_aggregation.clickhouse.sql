SELECT 
      coalesce(sum(v0.age), 0) AS "total_age"
FROM test_integration.users AS v0

-- column total_age: Value [("total_age", "total_age")]