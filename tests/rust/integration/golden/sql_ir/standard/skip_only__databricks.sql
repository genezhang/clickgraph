SELECT 
      u.full_name AS `u.name`
FROM social.users_bench AS u
ORDER BY u.full_name ASC
OFFSET 3