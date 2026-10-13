SELECT 
      v0.name AS `n.name`
FROM test_integration.users AS v0
WHERE CASE v0.name WHEN 'Alice' THEN 1 WHEN 'Bob' THEN 1 ELSE 0 END = 1
ORDER BY v0.name ASC NULLS LAST

-- column n.name: Value [("n.name", "n.name")]