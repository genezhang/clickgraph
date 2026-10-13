SELECT DISTINCT 
      v0.full_name AS `u.name`, 
      v4.content AS `p.content`
FROM db_standard.users AS v0
JOIN db_standard.user_follows AS v1 ON v1.follower_id = v0.user_id
JOIN db_standard.users AS v2 ON v1.followed_id = v2.user_id
JOIN db_standard.posts AS v3 ON v3.user_id = v2.user_id
JOIN db_standard.posts AS v4 ON v3.post_id = v4.post_id

-- column u.name: Value [("u.name", "u.name")]
-- column p.content: Value [("p.content", "p.content")]