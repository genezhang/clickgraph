SELECT 
      u.user_id AS "u.user_id", 
      if((toUInt8(u.age_str) < 18), 'minor', if((toUInt8(u.age_str) >= 65), 'senior', 'adult')) AS "u.age_group"
FROM test_integration.users_expressions_test AS u
WHERE u.user_id IN [11, 3, 12]
ORDER BY u.user_id ASC
