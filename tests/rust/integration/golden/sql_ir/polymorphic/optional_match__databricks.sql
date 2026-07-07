SELECT 
      u.full_name AS `u.name`, 
      p.content AS `p.title`
FROM brahmand.users_bench AS u
LEFT JOIN (SELECT * FROM brahmand.interactions WHERE interaction_type = 'AUTHORED' AND from_type = 'User' AND to_type = 'Post') AS t0 ON t0.from_id = u.user_id
LEFT JOIN brahmand.posts_bench AS p ON p.post_id = t0.to_id
