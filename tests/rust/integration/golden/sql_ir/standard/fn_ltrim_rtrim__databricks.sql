SELECT 
      ltrim(u.full_name) AS `l`, 
      rtrim(u.full_name) AS `r`
FROM social.users_bench AS u
