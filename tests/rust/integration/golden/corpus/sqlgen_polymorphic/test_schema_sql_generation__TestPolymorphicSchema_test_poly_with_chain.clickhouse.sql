WITH with_posts_u_cte_0 AS (SELECT 
      anyLast(u.full_name) AS "p1_u_name", 
      count(t0.to_id) AS "posts"
FROM db_polymorphic.users AS u
INNER JOIN db_polymorphic.interactions AS t0 ON t0.from_id = u.user_id AND t0.interaction_type = 'AUTHORED' AND t0.from_type = 'User' AND t0.to_type = 'Post'
GROUP BY u.user_id
)
SELECT 
      posts_u.p1_u_name AS "u.name", 
      posts_u.posts AS "posts"
FROM with_posts_u_cte_0 AS posts_u
ORDER BY posts_u.posts DESC
