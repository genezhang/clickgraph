SELECT 
      u.user_id AS `u.user_id`, 
      CAST(u.user_id > 2 AS BOOLEAN) AS `gt`, 
      CAST(startswith(u.full_name, 'A') AS BOOLEAN) AS `sw`, 
      CAST(u.email_address IS NULL AS BOOLEAN) AS `isn`
FROM social.users_bench AS u
WHERE u.user_id > 0
