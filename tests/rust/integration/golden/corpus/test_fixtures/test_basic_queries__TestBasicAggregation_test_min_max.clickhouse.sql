SELECT 
      minOrNull(u.age) AS "min_age", 
      maxOrNull(u.age) AS "max_age"
FROM test_integration.users AS u
