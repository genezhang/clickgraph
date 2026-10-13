SELECT 
      dateDiff('day', v0.registration_date, today()) AS "u.account_age_days"
FROM test_integration.users_expressions_test AS v0
WHERE v0.user_id = 1

-- column u.account_age_days: Value [("u.account_age_days", "u.account_age_days")]