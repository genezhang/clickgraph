SELECT 
      a.name AS "a.name"
FROM test_integration.users AS a
WHERE (a.age > 30 AND a.age < 25)
