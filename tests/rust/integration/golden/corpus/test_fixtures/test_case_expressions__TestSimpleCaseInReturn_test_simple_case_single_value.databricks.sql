SELECT 
      v0.name AS `n.name`, 
      CASE v0.name WHEN 'Alice' THEN 'Admin' ELSE 'User' END AS `role`
FROM test_integration.users AS v0
ORDER BY v0.name ASC NULLS LAST

-- column n.name: Value [("n.name", "n.name")]
-- column role: Value [("role", "role")]