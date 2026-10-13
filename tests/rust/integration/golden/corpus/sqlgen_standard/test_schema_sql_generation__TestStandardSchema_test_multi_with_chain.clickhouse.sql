WITH with_w1 AS (
SELECT 
      v0.user_id AS "v3__user_id", 
      v0.full_name AS "p2_v3_name", 
      count(v2.post_id) AS "v4"
FROM db_standard.users AS v0
JOIN db_standard.posts AS v1 ON v1.user_id = v0.user_id
JOIN db_standard.posts AS v2 ON v1.post_id = v2.post_id
GROUP BY v0.user_id, v0.full_name
)
SELECT 
      w1.p2_v3_name AS "u.name", 
      w1.v4 AS "posts", 
      count(v6.user_id) AS "following"
FROM with_w1 AS w1
JOIN db_standard.user_follows AS v5 ON v5.follower_id = w1.v3__user_id
JOIN db_standard.users AS v6 ON v5.followed_id = v6.user_id
GROUP BY w1.p2_v3_name, w1.v4

-- column u.name: Value [("u.name", "u.name")]
-- column posts: Value [("posts", "posts")]
-- column following: Value [("following", "following")]