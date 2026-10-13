SELECT 
      v0.name AS "a.name", 
      v1.name AS "b.name"
FROM test_integration.users AS v0
CROSS JOIN test_integration.users AS v1
WHERE (v0.name = 'Alice' AND v1.name = 'Bob')
