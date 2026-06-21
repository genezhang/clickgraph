SELECT 
      u.country AS "u.country", 
      groupArray(u.full_name) AS "names"
FROM social.users_bench AS u
GROUP BY u.country
