SELECT 
      v0.name AS `n.name`, 
      v0.age AS `n.age`, 
      CASE WHEN v0.age < 25 THEN 'Young' ELSE CASE WHEN v0.age < 35 THEN 'Adult' ELSE 'Senior' END END AS `category`
FROM test_integration.users AS v0
ORDER BY v0.name ASC NULLS LAST
