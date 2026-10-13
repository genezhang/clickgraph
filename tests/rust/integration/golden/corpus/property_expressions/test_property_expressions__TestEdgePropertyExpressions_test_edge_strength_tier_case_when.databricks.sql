SELECT 
      count(*) AS `strong_relationships`
FROM test_integration.users_expressions_test AS v0
JOIN test_integration.follows_expressions_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_expressions_test AS v2 ON v1.followed_id = v2.user_id
WHERE if((v1.interaction_count >= 100), 'strong', if((v1.interaction_count >= 50), 'medium', 'weak')) = 'strong'

-- column strong_relationships: Value [("strong_relationships", "strong_relationships")]