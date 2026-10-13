SELECT 
      v0.name AS `n.name`, 
      CASE WHEN v0.age > 1000 THEN 'Ancient' WHEN v0.age < 0 THEN 'Invalid' END AS `impossible_category`
FROM test_integration.users AS v0
ORDER BY v0.name ASC NULLS LAST
