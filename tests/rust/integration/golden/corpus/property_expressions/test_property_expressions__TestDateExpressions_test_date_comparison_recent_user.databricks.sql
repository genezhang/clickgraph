SELECT 
      v0.user_id AS `u.user_id`, 
      concat(v0.first_name, ' ', v0.last_name) AS `u.full_name`
FROM test_integration.users_expressions_test AS v0
WHERE dateDiff('day', v0.registration_date, today()) <= 30
ORDER BY v0.user_id ASC NULLS LAST

-- column u.user_id: Value [("u.user_id", "u.user_id")]
-- column u.full_name: Value [("u.full_name", "u.full_name")]