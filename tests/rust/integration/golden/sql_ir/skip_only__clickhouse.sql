SELECT 
      u.full_name AS "u.name"
FROM social.users_bench AS u
ORDER BY u.full_name ASC
LIMIT 3, 18446744073709551615