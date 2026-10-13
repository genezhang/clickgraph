SELECT 
      coalesce(sum(CASE WHEN v0.age < 30 THEN 1 WHEN v0.age < 40 THEN 2 ELSE 3 END), 0) AS `weighted_sum`
FROM test_integration.users AS v0
