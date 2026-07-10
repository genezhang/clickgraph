SELECT 
      n.name AS "n.name", 
      CASE WHEN n.age > 100 THEN 'Very old' ELSE NULL END AS "special_status"
FROM test_integration.users AS n
ORDER BY n.name ASC
