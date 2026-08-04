SELECT 
      u.user_id AS `u.user_id`
FROM test_integration.users_test AS u
WHERE u.full_name IS NOT NULL
