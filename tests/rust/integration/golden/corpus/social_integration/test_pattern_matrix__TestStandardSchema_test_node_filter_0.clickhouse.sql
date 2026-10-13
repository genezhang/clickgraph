SELECT 
      v0.email_address AS "n.email"
FROM test_integration.users_test AS v0
WHERE v0.email_address IS NOT NULL
LIMIT 10
-- column n.email: Value [("n.email", "n.email")]