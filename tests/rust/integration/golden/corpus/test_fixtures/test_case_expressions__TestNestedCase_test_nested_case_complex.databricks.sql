SELECT 
      n.name AS `n.name`, 
      n.age AS `n.age`, 
      CASE WHEN n.age >= 30 THEN CASE n.name WHEN 'Alice' THEN 'Senior Admin' ELSE 'Senior User' END ELSE CASE WHEN n.age >= 25 THEN 'Regular User' ELSE 'Junior User' END END AS `role`
FROM test_integration.users AS n
ORDER BY n.name ASC NULLS LAST
