SELECT 
      (v0.score + 100) AS `u.bonus_score`
FROM test_integration.users_expressions_test AS v0
WHERE v0.user_id = 5
