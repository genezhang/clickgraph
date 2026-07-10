SELECT 
      n.name AS `n.name`, 
      n.age AS `n.age`, 
      CASE WHEN n.age < 25 THEN 'Young' ELSE CASE WHEN n.age < 35 THEN 'Adult' ELSE 'Senior' END END AS `category`
FROM test_integration.users AS n
ORDER BY n.name ASC
