SELECT 
      count(*) AS `strong_relationships`
FROM test_integration.follows_expressions_test AS f
WHERE if((f.interaction_count >= 100), 'strong', if((f.interaction_count >= 50), 'medium', 'weak')) = 'strong'
