SELECT 
      n.name AS `n.name`, 
      CASE n.name WHEN 'Alice' THEN 'VIP' ELSE NULL END AS `status`
FROM test_integration.users AS n
ORDER BY n.name ASC NULLS LAST
