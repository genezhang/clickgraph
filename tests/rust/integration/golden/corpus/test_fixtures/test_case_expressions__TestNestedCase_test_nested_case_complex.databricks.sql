SELECT 
      v0.name AS `n.name`, 
      v0.age AS `n.age`, 
      CASE WHEN v0.age >= 30 THEN CASE v0.name WHEN 'Alice' THEN 'Senior Admin' ELSE 'Senior User' END ELSE CASE WHEN v0.age >= 25 THEN 'Regular User' ELSE 'Junior User' END END AS `role`
FROM test_integration.users AS v0
ORDER BY v0.name ASC NULLS LAST
