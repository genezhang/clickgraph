SELECT 
      (v0.score / 1000) AS "u.score_normalized"
FROM test_integration.users_expressions_test AS v0
WHERE v0.user_id = 2

-- column u.score_normalized: Value [("u.score_normalized", "u.score_normalized")]