SELECT 
      a.name AS `a.name`, 
      b.name AS `b.name`
FROM test_integration.users AS a
WHERE (a.name = 'Alice' AND b.name = 'Bob')
