SELECT 
      v0.is_active AS "n.is_active"
FROM test_integration.users_test AS v0
WHERE v0.is_active IS NOT NULL
ORDER BY v0.is_active DESC NULLS FIRST
LIMIT 5, 10