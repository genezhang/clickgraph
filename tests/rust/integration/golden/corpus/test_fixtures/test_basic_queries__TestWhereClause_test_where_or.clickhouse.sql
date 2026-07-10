SELECT 
      u.name AS "u.name"
FROM test_integration.users AS u
WHERE (u.name = 'Alice' OR u.name = 'Bob')
ORDER BY u.name ASC
