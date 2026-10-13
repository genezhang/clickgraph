SELECT 
      v0.user_id AS "u.user_id", 
      if((toUInt8(v0.age_str) < 18), 'minor', if((toUInt8(v0.age_str) >= 65), 'senior', 'adult')) AS "u.age_group"
FROM test_integration.users_expressions_test AS v0
WHERE v0.user_id IN [11, 3, 12]
ORDER BY v0.user_id ASC

-- column u.user_id: Value [("u.user_id", "u.user_id")]
-- column u.age_group: Value [("u.age_group", "u.age_group")]