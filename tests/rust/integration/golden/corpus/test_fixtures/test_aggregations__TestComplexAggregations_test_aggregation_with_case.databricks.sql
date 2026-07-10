SELECT 
      count(CASE WHEN n.age < 30 THEN 1 END) AS `young`, 
      count(CASE WHEN n.age >= 30 THEN 1 END) AS `mature`
FROM test_integration.users AS n
