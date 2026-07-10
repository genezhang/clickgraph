SELECT 
      n.name AS "n.name", 
      n.age AS "n.age", 
      CASE WHEN n.age > 20 THEN 'First' WHEN n.age > 25 THEN 'Second' WHEN n.age > 30 THEN 'Third' ELSE 'Last' END AS "result"
FROM test_integration.users AS n
ORDER BY n.name ASC
