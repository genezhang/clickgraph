SELECT 
      v0.name AS "u.name"
FROM test_integration.users AS v0
WHERE (v0.age > 25 AND v0.age < 32)
ORDER BY v0.name ASC
