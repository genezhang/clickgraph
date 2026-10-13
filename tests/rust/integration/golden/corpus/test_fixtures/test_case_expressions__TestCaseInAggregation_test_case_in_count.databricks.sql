SELECT 
      count(CASE WHEN v0.age < 30 THEN 1 END) AS `young_count`, 
      count(CASE WHEN v0.age >= 30 THEN 1 END) AS `mature_count`
FROM test_integration.users AS v0

-- column young_count: Value [("young_count", "young_count")]
-- column mature_count: Value [("mature_count", "mature_count")]