SELECT 
      v0.full_name AS `u.name`, 
      v2.full_name AS `f.name`
FROM db_standard.users AS v0
JOIN db_standard.user_follows AS v1 ON v1.follower_id = v0.user_id
JOIN db_standard.users AS v2 ON v1.followed_id = v2.user_id
