SELECT 
      u.full_name AS `u.name`
FROM test_integration.users_test AS u
WHERE (u.country = 'USA' AND u.is_active = true)
