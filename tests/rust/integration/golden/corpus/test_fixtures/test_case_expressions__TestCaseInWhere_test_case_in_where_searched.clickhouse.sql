SELECT 
      v0.name AS "n.name", 
      v0.age AS "n.age"
FROM test_integration.users AS v0
WHERE CASE WHEN v0.age < 25 THEN 'include' WHEN v0.age > 35 THEN 'include' ELSE 'exclude' END = 'include'
ORDER BY v0.name ASC
