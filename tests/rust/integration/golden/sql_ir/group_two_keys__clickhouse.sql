SELECT 
      u.country AS "u.country", 
      u.city AS "u.city", 
      count(u.user_id) AS "n"
FROM social.users_bench AS u
GROUP BY u.country, u.city
