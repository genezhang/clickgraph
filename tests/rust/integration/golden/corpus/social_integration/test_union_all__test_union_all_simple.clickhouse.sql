SELECT 
      u.full_name AS "name"
FROM test_integration.users_test AS u
WHERE u.user_id = 1
UNION ALL 
SELECT 
      u.full_name AS "name"
FROM test_integration.users_test AS u
WHERE u.user_id = 2
