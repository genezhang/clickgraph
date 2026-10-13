SELECT 
      if((v0.score >= 1000), 'gold', if((v0.score >= 500), 'silver', 'bronze')) AS "u.tier", 
      count(*) AS "count", 
      avg((v0.score / 1000)) AS "avg_norm_score"
FROM test_integration.users_expressions_test AS v0
GROUP BY if((v0.score >= 1000), 'gold', if((v0.score >= 500), 'silver', 'bronze'))
ORDER BY if((v0.score >= 1000), 'gold', if((v0.score >= 500), 'silver', 'bronze')) ASC
