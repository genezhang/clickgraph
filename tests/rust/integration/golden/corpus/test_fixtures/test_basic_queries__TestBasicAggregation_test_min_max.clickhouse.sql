SELECT 
      minOrNull(v0.age) AS "min_age", 
      maxOrNull(v0.age) AS "max_age"
FROM test_integration.users AS v0
