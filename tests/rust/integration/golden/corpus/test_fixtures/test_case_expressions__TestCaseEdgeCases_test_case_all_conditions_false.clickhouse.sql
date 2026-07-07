SELECT 
      n.name AS "n.name", 
      CASE WHEN n.age > 1000 THEN 'Ancient' WHEN n.age < 0 THEN 'Invalid' END AS "impossible_category"
FROM test_integration.users AS n
ORDER BY n.name ASC
