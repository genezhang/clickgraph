SELECT 
      count(*) AS `premium_count`
FROM test_integration.users_expressions_test AS v0
WHERE v0.is_premium = true
