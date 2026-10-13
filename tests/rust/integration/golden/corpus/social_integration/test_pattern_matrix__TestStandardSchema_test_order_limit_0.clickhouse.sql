SELECT 
      v0.email_address AS "n.email"
FROM test_integration.users_test AS v0
WHERE v0.email_address IS NOT NULL
ORDER BY v0.email_address DESC NULLS FIRST
LIMIT 5, 10