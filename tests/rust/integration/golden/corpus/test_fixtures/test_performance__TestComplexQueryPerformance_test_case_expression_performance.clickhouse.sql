SELECT 
      n.name AS "n.name", 
      CASE WHEN n.age < 25 THEN 'Young' WHEN n.age < 35 THEN 'Adult' ELSE 'Senior' END AS "age_group"
FROM test_integration.users AS n
ORDER BY n.name ASC
