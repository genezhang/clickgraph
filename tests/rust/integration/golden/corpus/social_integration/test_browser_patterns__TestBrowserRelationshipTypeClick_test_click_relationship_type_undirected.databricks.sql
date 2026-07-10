SELECT `r.from_id` AS `r.from_id`, `r.to_id` AS `r.to_id`, `r.follow_date` AS `r.follow_date` FROM (
SELECT 
      r.follower_id AS `r.from_id`, 
      r.followed_id AS `r.to_id`, 
      r.follow_date AS `r.follow_date`
FROM test_integration.user_follows_test AS r
UNION ALL 
SELECT 
      r.follower_id AS `r.from_id`, 
      r.followed_id AS `r.to_id`, 
      r.follow_date AS `r.follow_date`
FROM test_integration.user_follows_test AS r
) AS __union
LIMIT 25