SELECT 
      v0.name AS "n.name", 
      CASE WHEN v0.age > 100 THEN 'Very old' ELSE NULL END AS "special_status"
FROM test_integration.users AS v0
ORDER BY v0.name ASC
