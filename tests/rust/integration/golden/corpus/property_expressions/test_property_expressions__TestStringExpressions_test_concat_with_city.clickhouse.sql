SELECT 
      concat(v0.first_name, ' (', v0.city, ')') AS "u.display_name"
FROM test_integration.users_expressions_test AS v0
WHERE v0.user_id = 2
