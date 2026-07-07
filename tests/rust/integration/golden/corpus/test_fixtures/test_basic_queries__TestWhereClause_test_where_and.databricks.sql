SELECT 
      u.name AS `u.name`
FROM test_integration.users AS u
WHERE (u.age > 25 AND u.age < 32)
ORDER BY u.name ASC
