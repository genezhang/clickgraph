SELECT 
      (u.score + 100) AS "u.bonus_score"
FROM test_integration.users_expressions_test AS u
WHERE u.user_id = 5
