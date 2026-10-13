SELECT 
      v0.name AS `n.name`, 
      v0.age AS `n.age`, 
      CASE WHEN v0.age * 2 > 60 THEN 'High' WHEN v0.age + 10 < 30 THEN 'Low' ELSE 'Medium' END AS `calculated_category`
FROM test_integration.users AS v0
ORDER BY v0.name ASC NULLS LAST
