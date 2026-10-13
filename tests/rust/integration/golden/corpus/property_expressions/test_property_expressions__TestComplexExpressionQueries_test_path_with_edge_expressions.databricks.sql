SELECT 
      concat(v0.first_name, ' ', v0.last_name) AS `u1.full_name`, 
      concat(v2.first_name, ' ', v2.last_name) AS `u2.full_name`, 
      if((v1.interaction_count >= 100), 'strong', if((v1.interaction_count >= 50), 'medium', 'weak')) AS `f.strength_tier`
FROM test_integration.users_expressions_test AS v0
JOIN test_integration.follows_expressions_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_expressions_test AS v2 ON v1.followed_id = v2.user_id
WHERE ((if((v0.score >= 1000), 'gold', if((v0.score >= 500), 'silver', 'bronze')) = 'gold' AND (dateDiff('day', v1.follow_date, today()) <= 7) = true) AND if((v1.interaction_count >= 100), 'strong', if((v1.interaction_count >= 50), 'medium', 'weak')) IN ('strong', 'moderate'))
