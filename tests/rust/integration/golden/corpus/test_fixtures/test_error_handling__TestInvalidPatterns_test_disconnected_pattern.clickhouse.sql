SELECT 
      a.name AS "a.name", 
      b.name AS "b.name"
FROM test_integration.users AS a
JOIN test_integration.users AS b ON 1 = 1
WHERE (a.name = 'Alice' AND b.name = 'Bob')
