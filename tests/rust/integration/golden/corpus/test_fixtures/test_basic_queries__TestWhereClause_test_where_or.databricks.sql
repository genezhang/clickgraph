SELECT 
      v0.name AS `u.name`
FROM test_integration.users AS v0
WHERE (v0.name = 'Alice' OR v0.name = 'Bob')
ORDER BY v0.name ASC NULLS LAST
