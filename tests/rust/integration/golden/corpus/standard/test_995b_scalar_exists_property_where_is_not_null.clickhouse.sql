SELECT 
      v0.user_id AS "u.user_id"
FROM test_integration.users_test AS v0
WHERE v0.full_name IS NOT NULL

-- column u.user_id: Value [("u.user_id", "u.user_id")]