SELECT 
      toUInt8(u.age_str) AS `u.age_int`
FROM test_integration.users_expressions_test AS u
WHERE u.user_id = 1
