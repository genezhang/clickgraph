SELECT 
      n.name AS "n.name", 
      n.age AS "n.age"
FROM test_integration.users AS n
WHERE CASE WHEN n.age < 25 THEN 'include' WHEN n.age > 35 THEN 'include' ELSE 'exclude' END = 'include'
ORDER BY n.name ASC
