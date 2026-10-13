SELECT 
      min(v0.age) AS `min_age`, 
      max(v0.age) AS `max_age`
FROM test_integration.users AS v0

-- column min_age: Value [("min_age", "min_age")]
-- column max_age: Value [("max_age", "max_age")]