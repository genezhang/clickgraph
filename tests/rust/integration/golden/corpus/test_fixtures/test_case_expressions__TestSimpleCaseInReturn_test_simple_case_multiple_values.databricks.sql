SELECT 
      v0.name AS `n.name`, 
      CASE v0.name WHEN 'Alice' THEN 'Level 3' WHEN 'Bob' THEN 'Level 2' WHEN 'Charlie' THEN 'Level 2' ELSE 'Level 1' END AS `level`
FROM test_integration.users AS v0
ORDER BY v0.name ASC NULLS LAST

-- column n.name: Value [("n.name", "n.name")]
-- column level: Value [("level", "level")]