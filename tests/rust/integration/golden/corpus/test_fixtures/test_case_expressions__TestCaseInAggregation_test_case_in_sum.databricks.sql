SELECT 
      sum(CASE WHEN n.age < 30 THEN 1 WHEN n.age < 40 THEN 2 ELSE 3 END) AS `weighted_sum`
FROM test_integration.users AS n
