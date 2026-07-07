WITH with_posts_u_cte_0 AS (SELECT 
      any_value(u.full_name) AS `p1_u_name`, 
      u.user_id AS `p1_u_user_id`, 
      count(p.post_id) AS `posts`
FROM db_standard.posts AS p
INNER JOIN db_standard.users AS u ON u.user_id = p.user_id
GROUP BY u.user_id
)
SELECT 
      u.p1_u_name AS `u.name`, 
      any_value(u.posts) AS `posts`, 
      count(t0.followed_id) AS `following`
FROM with_posts_u_cte_0 AS u
INNER JOIN db_standard.user_follows AS t0 ON t0.follower_id = u.p1_u_user_id
GROUP BY u.p1_u_name, u.posts
