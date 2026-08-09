SELECT 
      n.name AS `n.name`
FROM test_integration.users AS n
WHERE CASE n.name WHEN 'Alice' THEN 1 WHEN 'Bob' THEN 1 ELSE 0 END = 1
ORDER BY n.name ASC NULLS LAST
