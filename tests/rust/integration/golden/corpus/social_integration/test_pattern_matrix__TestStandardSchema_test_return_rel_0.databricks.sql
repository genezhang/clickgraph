SELECT 
      r.follower_id AS `r.from_id`, 
      r.followed_id AS `r.to_id`, 
      r.follow_date AS `r.follow_date`
FROM test_integration.user_follows_test AS r
LIMIT 5