SELECT 
      u.user_id AS `u.user_id`
FROM brahmand.users_bench AS u
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = u.user_id AND t0.interaction_type = 'LIKES' AND t0.from_type = 'User' AND t0.to_type = 'Post'
INNER JOIN brahmand.posts_bench AS p ON p.post_id = t0.to_id
WHERE (u.full_name = 'Alice Smith' AND p.content = 'Hello world!')
