SELECT 
      n.city AS `n.city`, 
      n.country AS `n.country`, 
      n.email_address AS `n.email`, 
      n.is_active AS `n.is_active`, 
      n.full_name AS `n.name`, 
      n.registration_date AS `n.registration_date`, 
      n.user_id AS `n.user_id`, 
      o.city AS `o.city`, 
      o.country AS `o.country`, 
      o.email_address AS `o.email`, 
      o.is_active AS `o.is_active`, 
      o.full_name AS `o.name`, 
      o.registration_date AS `o.registration_date`, 
      o.user_id AS `o.user_id`
FROM social.users_bench AS n
INNER JOIN social.user_follows_bench AS r ON r.follower_id = n.user_id
INNER JOIN social.users_bench AS o ON o.user_id = r.followed_id
UNION ALL 
SELECT 
      n.city AS `n.city`, 
      n.country AS `n.country`, 
      n.email_address AS `n.email`, 
      n.is_active AS `n.is_active`, 
      n.full_name AS `n.name`, 
      n.registration_date AS `n.registration_date`, 
      n.user_id AS `n.user_id`, 
      o.city AS `o.city`, 
      o.country AS `o.country`, 
      o.email_address AS `o.email`, 
      o.is_active AS `o.is_active`, 
      o.full_name AS `o.name`, 
      o.registration_date AS `o.registration_date`, 
      o.user_id AS `o.user_id`
FROM social.users_bench AS o
INNER JOIN social.user_follows_bench AS r ON r.follower_id = o.user_id
INNER JOIN social.users_bench AS n ON n.user_id = r.followed_id
