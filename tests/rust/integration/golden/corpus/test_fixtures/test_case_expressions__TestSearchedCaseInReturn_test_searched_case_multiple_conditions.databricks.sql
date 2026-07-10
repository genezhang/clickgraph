SELECT 
      n.name AS `n.name`, 
      n.age AS `n.age`, 
      CASE WHEN (n.age > 30 AND n.name = 'Alice') THEN 'Senior Admin' WHEN n.age > 30 THEN 'Senior User' WHEN n.age > 25 THEN 'Regular User' ELSE 'Junior User' END AS `category`
FROM test_integration.users AS n
ORDER BY n.name ASC
