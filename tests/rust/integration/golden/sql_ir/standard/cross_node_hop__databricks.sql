SELECT 
      u.full_name AS `u.name`, 
      p.post_title AS `p.title`
FROM social.users_bench AS u
INNER JOIN social.authored_bench AS t0 ON t0.user_id = u.user_id
INNER JOIN social.posts_bench AS p ON p.post_id = t0.post_id
