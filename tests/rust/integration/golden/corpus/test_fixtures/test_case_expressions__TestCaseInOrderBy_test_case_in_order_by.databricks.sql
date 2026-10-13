SELECT 
      v0.name AS `n.name`, 
      v0.age AS `n.age`
FROM test_integration.users AS v0
ORDER BY CASE WHEN v0.name = 'Alice' THEN 1 WHEN v0.name = 'Bob' THEN 2 ELSE 3 END ASC NULLS LAST, v0.name ASC NULLS LAST

-- column n.name: Value [("n.name", "n.name")]
-- column n.age: Value [("n.age", "n.age")]