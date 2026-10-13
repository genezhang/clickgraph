SELECT 
      count(CASE WHEN v0.age < 30 THEN 1 END) AS "young_count", 
      count(CASE WHEN v0.age >= 30 THEN 1 END) AS "mature_count"
FROM test_integration.users AS v0
