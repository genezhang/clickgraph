SELECT 
      upper(u.full_name) AS `up`, 
      lower(u.country) AS `lo`
FROM social.users_bench AS u
