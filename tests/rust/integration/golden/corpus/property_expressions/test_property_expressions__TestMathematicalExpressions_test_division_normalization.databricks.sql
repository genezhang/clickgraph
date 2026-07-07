SELECT 
      (u.score / 1000) AS `u.score_normalized`
FROM test_integration.users_expressions_test AS u
WHERE u.user_id = 2
