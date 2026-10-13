SELECT 
      count(v0.user_id) AS "total", 
      avg(v0.age) AS "avg_age", 
      minOrNull(v0.age) AS "min_age", 
      maxOrNull(v0.age) AS "max_age"
FROM test_integration.users AS v0

-- column total: Value [("total", "total")]
-- column avg_age: Value [("avg_age", "avg_age")]
-- column min_age: Value [("min_age", "min_age")]
-- column max_age: Value [("max_age", "max_age")]