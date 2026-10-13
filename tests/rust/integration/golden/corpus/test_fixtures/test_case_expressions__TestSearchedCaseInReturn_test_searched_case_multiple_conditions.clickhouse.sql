SELECT 
      v0.name AS "n.name", 
      v0.age AS "n.age", 
      CASE WHEN (v0.age > 30 AND v0.name = 'Alice') THEN 'Senior Admin' WHEN v0.age > 30 THEN 'Senior User' WHEN v0.age > 25 THEN 'Regular User' ELSE 'Junior User' END AS "category"
FROM test_integration.users AS v0
ORDER BY v0.name ASC

-- column n.name: Value [("n.name", "n.name")]
-- column n.age: Value [("n.age", "n.age")]
-- column category: Value [("category", "category")]