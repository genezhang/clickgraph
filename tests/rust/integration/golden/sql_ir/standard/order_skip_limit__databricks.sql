SELECT 
      u.full_name AS `u.name`
FROM social.users_bench AS u
ORDER BY u.full_name DESC NULLS FIRST
LIMIT 10 OFFSET 5