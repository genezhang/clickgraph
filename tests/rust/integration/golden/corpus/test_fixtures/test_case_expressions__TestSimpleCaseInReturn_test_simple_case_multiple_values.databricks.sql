SELECT 
      n.name AS `n.name`, 
      CASE n.name WHEN 'Alice' THEN 'Level 3' WHEN 'Bob' THEN 'Level 2' WHEN 'Charlie' THEN 'Level 2' ELSE 'Level 1' END AS `level`
FROM test_integration.users AS n
ORDER BY n.name ASC
