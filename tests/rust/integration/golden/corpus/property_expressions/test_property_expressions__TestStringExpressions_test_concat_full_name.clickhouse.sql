SELECT 
      concat(v0.first_name, ' ', v0.last_name) AS "u.full_name"
FROM test_integration.users_expressions_test AS v0
WHERE v0.user_id = 1
