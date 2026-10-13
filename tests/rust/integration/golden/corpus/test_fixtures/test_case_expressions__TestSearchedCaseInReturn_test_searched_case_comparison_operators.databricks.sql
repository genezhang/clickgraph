SELECT 
      v0.name AS `n.name`, 
      v0.age AS `n.age`, 
      CASE WHEN v0.age = 30 THEN 'Exactly 30' WHEN v0.age > 30 THEN 'Over 30' WHEN v0.age >= 25 THEN '25-29' ELSE 'Under 25' END AS `age_category`
FROM test_integration.users AS v0
ORDER BY v0.name ASC NULLS LAST
