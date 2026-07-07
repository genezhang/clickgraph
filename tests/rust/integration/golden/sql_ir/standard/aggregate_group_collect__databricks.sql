SELECT 
      u.country AS `u.country`, 
      collect_list(u.full_name) AS `names`
FROM social.users_bench AS u
GROUP BY u.country
