SELECT 
      if((u.score >= 1000), 'gold', if((u.score >= 500), 'silver', 'bronze')) AS "u.tier", 
      count(*) AS "count", 
      avg((u.score / 1000)) AS "avg_norm_score"
FROM test_integration.users_expressions_test AS u
GROUP BY if((u.score >= 1000), 'gold', if((u.score >= 500), 'silver', 'bronze'))
ORDER BY if((u.score >= 1000), 'gold', if((u.score >= 500), 'silver', 'bronze')) ASC
