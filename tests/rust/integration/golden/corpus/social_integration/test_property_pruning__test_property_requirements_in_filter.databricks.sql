SELECT 
      v0.full_name AS `u.name`
FROM test_integration.users_test AS v0
WHERE (v0.country = 'USA' AND v0.is_active = true)
