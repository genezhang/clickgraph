SELECT 
      CASE WHEN v0.age < 30 THEN 'Young' ELSE 'Mature' END AS `age_group`, 
      count(v0.user_id) AS `count`
FROM test_integration.users AS v0
GROUP BY CASE WHEN v0.age < 30 THEN 'Young' ELSE 'Mature' END
ORDER BY CASE WHEN v0.age < 30 THEN 'Young' ELSE 'Mature' END ASC NULLS LAST
