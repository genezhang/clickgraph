SELECT 
      u.user_id AS `u.user_id`, 
      concat(u.first_name, ' ', u.last_name) AS `u.full_name`
FROM test_integration.users_expressions_test AS u
WHERE dateDiff('day', u.registration_date, today()) <= 30
ORDER BY u.user_id ASC
