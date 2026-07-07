SELECT 
      n.name AS "n.name", 
      n.age AS "n.age"
FROM test_integration.users AS n
ORDER BY CASE WHEN n.name = 'Alice' THEN 1 WHEN n.name = 'Bob' THEN 2 ELSE 3 END ASC, n.name ASC
