SELECT 
      min(n.age) AS `youngest`, 
      max(n.age) AS `oldest`
FROM test_integration.users AS n
