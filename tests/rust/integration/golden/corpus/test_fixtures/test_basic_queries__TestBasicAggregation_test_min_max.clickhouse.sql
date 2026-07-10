SELECT 
      min(u.age) AS "min_age", 
      max(u.age) AS "max_age"
FROM test_integration.users AS u
