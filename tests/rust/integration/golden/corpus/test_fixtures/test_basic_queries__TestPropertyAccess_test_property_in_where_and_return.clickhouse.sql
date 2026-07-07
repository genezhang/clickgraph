SELECT 
      u.name AS "u.name", 
      u.age AS "u.age"
FROM test_integration.users AS u
WHERE u.age > 30
ORDER BY u.age ASC
