SELECT 
      v0.name AS "n.name", 
      v0.age AS "n.age", 
      CASE WHEN v0.age > 20 THEN 'First' WHEN v0.age > 25 THEN 'Second' WHEN v0.age > 30 THEN 'Third' ELSE 'Last' END AS "result"
FROM test_integration.users AS v0
ORDER BY v0.name ASC
