SELECT 
      v0.name AS "n.name"
FROM test_integration.users AS v0
WHERE CASE WHEN v0.name = 'Alice' THEN 1 WHEN v0.name = 'Bob' THEN 1 ELSE 0 END = 1
ORDER BY v0.name ASC

-- column n.name: Value [("n.name", "n.name")]