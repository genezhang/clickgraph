SELECT 
      n.user_id AS `n.user_id`
FROM test_integration.users_test AS n
WHERE n.user_id IS NOT NULL
LIMIT 10