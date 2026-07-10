SELECT 
      toFloat64(u.score_str) AS "u.score_float"
FROM test_integration.users_expressions_test AS u
WHERE u.user_id = 1
