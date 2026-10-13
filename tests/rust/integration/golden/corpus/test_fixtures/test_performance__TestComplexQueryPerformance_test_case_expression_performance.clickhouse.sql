SELECT 
      v0.name AS "n.name", 
      CASE WHEN v0.age < 25 THEN 'Young' WHEN v0.age < 35 THEN 'Adult' ELSE 'Senior' END AS "age_group"
FROM test_integration.users AS v0
ORDER BY v0.name ASC

-- column n.name: Value [("n.name", "n.name")]
-- column age_group: Value [("age_group", "age_group")]