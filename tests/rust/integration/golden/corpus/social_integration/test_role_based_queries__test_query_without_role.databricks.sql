SELECT 
      u.full_name AS `u.name`, 
      u.email_address AS `u.email`
FROM test_integration.users_test AS u
WHERE u.user_id = 1
LIMIT 1