SELECT 
      n.name AS "n.name", 
      n.age AS "n.age", 
      CASE WHEN n.age = 30 THEN 'Exactly 30' WHEN n.age > 30 THEN 'Over 30' WHEN n.age >= 25 THEN '25-29' ELSE 'Under 25' END AS "age_category"
FROM test_integration.users AS n
ORDER BY n.name ASC
