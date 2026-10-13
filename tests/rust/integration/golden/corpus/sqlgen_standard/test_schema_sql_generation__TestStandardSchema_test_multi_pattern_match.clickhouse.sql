SELECT 
      v0.full_name AS "u.name", 
      count(DISTINCT v2.post_id) AS "posts", 
      count(DISTINCT v4.user_id) AS "following"
FROM db_standard.users AS v0
JOIN db_standard.posts AS v1 ON v1.user_id = v0.user_id
JOIN db_standard.posts AS v2 ON v1.post_id = v2.post_id
JOIN db_standard.user_follows AS v3 ON v3.follower_id = v0.user_id
JOIN db_standard.users AS v4 ON v3.followed_id = v4.user_id
GROUP BY v0.full_name
