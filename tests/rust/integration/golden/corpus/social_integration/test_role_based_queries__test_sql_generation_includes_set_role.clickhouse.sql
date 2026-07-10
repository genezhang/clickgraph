SELECT 
      u.full_name AS "u.name"
FROM test_integration.users_test AS u
WHERE u.user_id = 1
LIMIT 1