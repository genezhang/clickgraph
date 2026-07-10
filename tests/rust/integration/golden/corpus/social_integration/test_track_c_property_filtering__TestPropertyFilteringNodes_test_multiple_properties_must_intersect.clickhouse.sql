SELECT 
      n.user_id AS "uid", 
      n.email_address AS "email"
FROM test_integration.users_test AS n
WHERE (n.user_id = 1 AND n.email_address IS NOT NULL)
