SELECT 
      u.user_id AS `u.user_id`
FROM social.users_bench AS u
WHERE (position('a', u.full_name) > 0)
