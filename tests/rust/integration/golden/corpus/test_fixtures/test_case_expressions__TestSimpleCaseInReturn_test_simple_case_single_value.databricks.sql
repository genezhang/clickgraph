SELECT 
      n.name AS `n.name`, 
      CASE n.name WHEN 'Alice' THEN 'Admin' ELSE 'User' END AS `role`
FROM test_integration.users AS n
ORDER BY n.name ASC
