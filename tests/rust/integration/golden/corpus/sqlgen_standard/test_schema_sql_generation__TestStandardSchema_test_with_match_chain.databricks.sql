WITH with_posts_u_cte_0 AS (SELECT 
      any_value(u.full_name) AS `p1_u_name`, 
      count(p.post_id) AS `posts`
FROM db_standard.posts AS p
INNER JOIN db_standard.users AS u ON u.user_id = p.user_id
GROUP BY u.user_id
HAVING posts > 0
)
SELECT 
      posts_u.p1_u_name AS `u.name`, 
      posts_u.posts AS `posts`
FROM with_posts_u_cte_0 AS posts_u
