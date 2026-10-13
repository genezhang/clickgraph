SELECT 
      v0.name AS `n.name`, 
      CASE v0.name WHEN 'Alice' THEN 'VIP' ELSE NULL END AS `status`
FROM test_integration.users AS v0
ORDER BY v0.name ASC NULLS LAST
