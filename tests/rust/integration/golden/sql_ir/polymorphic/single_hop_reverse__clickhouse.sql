SELECT 
      p.content AS "p.title", 
      u.full_name AS "u.name"
FROM brahmand.users_bench AS u
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = u.user_id AND t0.interaction_type = 'AUTHORED' AND t0.from_type = 'User' AND t0.to_type = 'Post'
INNER JOIN brahmand.posts_bench AS p ON p.post_id = t0.to_id
