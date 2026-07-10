SELECT 
      concat(u1.first_name, ' ', u1.last_name) AS `u1.full_name`, 
      concat(u2.first_name, ' ', u2.last_name) AS `u2.full_name`, 
      if((f.interaction_count >= 100), 'strong', if((f.interaction_count >= 50), 'medium', 'weak')) AS `f.strength_tier`
FROM test_integration.users_expressions_test AS u1
INNER JOIN test_integration.follows_expressions_test AS f ON f.follower_id = u1.user_id
INNER JOIN test_integration.users_expressions_test AS u2 ON u2.user_id = f.followed_id
WHERE ((if((u1.score >= 1000), 'gold', if((u1.score >= 500), 'silver', 'bronze')) = 'gold' AND (dateDiff('day', f.follow_date, today()) <= 7) = true) AND if((f.interaction_count >= 100), 'strong', if((f.interaction_count >= 50), 'medium', 'weak')) IN ('strong', 'moderate'))
