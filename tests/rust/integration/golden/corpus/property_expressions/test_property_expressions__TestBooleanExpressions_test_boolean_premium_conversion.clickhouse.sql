SELECT 
      count(*) AS "premium_count"
FROM test_integration.users_expressions_test AS u
WHERE u.is_premium = true
