SELECT 
      dateDiff('day', u.registration_date, today()) AS `u.account_age_days`
FROM test_integration.users_expressions_test AS u
WHERE u.user_id = 1
