SELECT 
      v0.full_name AS `n.name`
FROM test_integration.users_test AS v0
WHERE v0.full_name IS NOT NULL
LIMIT 10