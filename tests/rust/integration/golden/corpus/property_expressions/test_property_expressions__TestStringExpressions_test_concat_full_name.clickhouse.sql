SELECT 
      concat(u.first_name, ' ', u.last_name) AS "u.full_name"
FROM test_integration.users_expressions_test AS u
WHERE u.user_id = 1
