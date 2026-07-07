SELECT 
      n.name AS `n.name`, 
      n.age AS `n.age`, 
      CASE WHEN n.age * 2 > 60 THEN 'High' WHEN n.age + 10 < 30 THEN 'Low' ELSE 'Medium' END AS `calculated_category`
FROM test_integration.users AS n
ORDER BY n.name ASC
