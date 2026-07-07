SELECT 
      n.user_id AS "n.user_id"
FROM test_integration.users_test AS n
WHERE n.user_id IS NOT NULL
ORDER BY n.user_id DESC
LIMIT 5, 10