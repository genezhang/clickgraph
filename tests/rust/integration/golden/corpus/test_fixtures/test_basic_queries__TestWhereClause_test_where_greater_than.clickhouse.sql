SELECT 
      u.name AS "u.name"
FROM test_integration.users AS u
WHERE u.age > 30
ORDER BY u.name ASC
