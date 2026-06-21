SELECT 
      u.user_id AS "u.user_id"
FROM social.users_bench AS u
WHERE (position(u.full_name, 'a') > 0)
