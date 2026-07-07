SELECT 
      n.email_address AS "n.email"
FROM test_integration.users_test AS n
WHERE n.email_address IS NOT NULL
ORDER BY n.email_address DESC
LIMIT 5, 10