SELECT 
      concat(u.first_name, ' (', u.city, ')') AS `u.display_name`
FROM test_integration.users_expressions_test AS u
WHERE u.user_id = 2
