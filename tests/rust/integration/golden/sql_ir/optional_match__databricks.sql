SELECT 
      u.full_name AS `u.name`, 
      p.post_title AS `p.title`
FROM social.users_bench AS u
LEFT JOIN social.authored_bench AS t0 ON t0.user_id = u.user_id
LEFT JOIN social.posts_bench AS p ON p.post_id = t0.post_id
