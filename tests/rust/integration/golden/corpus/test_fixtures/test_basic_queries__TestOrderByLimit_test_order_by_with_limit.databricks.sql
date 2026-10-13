SELECT 
      v0.name AS `u.name`
FROM test_integration.users AS v0
ORDER BY v0.age DESC NULLS FIRST
LIMIT 2
-- column u.name: Value [("u.name", "u.name")]