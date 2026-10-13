SELECT 
      toDate(v0.birth_date_str) AS "u.birth_date"
FROM test_integration.users_expressions_test AS v0
WHERE v0.user_id = 1

-- column u.birth_date: Value [("u.birth_date", "u.birth_date")]