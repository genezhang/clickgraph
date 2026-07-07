SELECT 
      CASE WHEN n.age < 30 THEN 'Young' ELSE 'Mature' END AS "age_group", 
      count(n.user_id) AS "count"
FROM test_integration.users AS n
GROUP BY CASE WHEN n.age < 30 THEN 'Young' ELSE 'Mature' END
ORDER BY age_group ASC
