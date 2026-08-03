SELECT 
      upperUTF8(u.full_name) AS "up", 
      lowerUTF8(u.country) AS "lo"
FROM social.users_bench AS u
