SELECT 
      toFloat64(v0.score_str) AS "u.score_float"
FROM test_integration.users_expressions_test AS v0
WHERE v0.user_id = 1

-- column u.score_float: Value [("u.score_float", "u.score_float")]