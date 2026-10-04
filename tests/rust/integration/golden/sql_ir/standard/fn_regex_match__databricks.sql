SELECT 
      u.user_id AS `u.user_id`
FROM social.users_bench AS u
WHERE rlike(u.full_name, '\\A(?:.*a.*)\\z')
