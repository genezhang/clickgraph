SELECT 
      toDate(u.birth_date_str) AS "u.birth_date"
FROM test_integration.users_expressions_test AS u
WHERE u.user_id = 1
