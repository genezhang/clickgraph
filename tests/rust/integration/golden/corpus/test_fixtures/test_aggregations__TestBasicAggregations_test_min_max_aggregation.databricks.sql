SELECT 
      min(v0.age) AS `youngest`, 
      max(v0.age) AS `oldest`
FROM test_integration.users AS v0
