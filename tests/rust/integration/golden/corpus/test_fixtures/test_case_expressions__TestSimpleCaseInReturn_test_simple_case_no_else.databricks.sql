SELECT 
      v0.name AS `n.name`, 
      CASE WHEN v0.name = 'Alice' THEN 'VIP' END AS `status`
FROM test_integration.users AS v0
ORDER BY v0.name ASC NULLS LAST

-- column n.name: Value [("n.name", "n.name")]
-- column status: Value [("status", "status")]