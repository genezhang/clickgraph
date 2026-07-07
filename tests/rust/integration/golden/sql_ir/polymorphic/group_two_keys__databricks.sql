SELECT 
      u.full_name AS `u.name`, 
      u.email_address AS `u.email`, 
      count(u.user_id) AS `n`
FROM brahmand.users_bench AS u
GROUP BY u.full_name, u.email_address
