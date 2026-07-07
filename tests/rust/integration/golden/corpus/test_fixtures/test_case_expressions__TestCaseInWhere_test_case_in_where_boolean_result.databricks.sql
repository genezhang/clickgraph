SELECT 
      n.name AS `n.name`, 
      n.age AS `n.age`
FROM test_integration.users AS n
WHERE CASE WHEN n.age >= 30 THEN true ELSE false END
ORDER BY n.name ASC
