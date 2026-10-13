SELECT 
      v0.full_name AS `u.name`, 
      v0.email_address AS `u.email`
FROM test_integration.users_test AS v0
WHERE v0.user_id = 1
LIMIT 1