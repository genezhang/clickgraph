SELECT 
      v0.name AS "u.name"
FROM test_integration.users AS v0
WHERE v0.age < 30
ORDER BY v0.name ASC
