SELECT `n.author_id` AS `n.author_id`, `n.city` AS `n.city`, `n.content` AS `n.content`, `n.country` AS `n.country`, `n.date` AS `n.date`, `n.email` AS `n.email`, `n.is_active` AS `n.is_active`, `n.name` AS `n.name`, `n.post_id` AS `n.post_id`, `n.registration_date` AS `n.registration_date`, `n.title` AS `n.title`, `n.user_id` AS `n.user_id` FROM (
SELECT 
      string(n.author_id) AS `n.author_id`, 
      NULL AS `n.city`, 
      string(n.post_content) AS `n.content`, 
      NULL AS `n.country`, 
      string(n.post_date) AS `n.date`, 
      NULL AS `n.email`, 
      NULL AS `n.is_active`, 
      NULL AS `n.name`, 
      string(n.post_id) AS `n.post_id`, 
      NULL AS `n.registration_date`, 
      string(n.post_title) AS `n.title`, 
      NULL AS `n.user_id`
FROM social.posts_bench AS n
UNION ALL 
SELECT 
      NULL AS `n.author_id`, 
      string(n.city) AS `n.city`, 
      NULL AS `n.content`, 
      string(n.country) AS `n.country`, 
      NULL AS `n.date`, 
      string(n.email_address) AS `n.email`, 
      string(n.is_active) AS `n.is_active`, 
      string(n.full_name) AS `n.name`, 
      NULL AS `n.post_id`, 
      string(n.registration_date) AS `n.registration_date`, 
      NULL AS `n.title`, 
      string(n.user_id) AS `n.user_id`
FROM social.users_bench AS n
) AS __union
LIMIT 25