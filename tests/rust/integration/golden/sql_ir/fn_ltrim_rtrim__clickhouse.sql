SELECT 
      trimLeft(u.full_name) AS "l", 
      trimRight(u.full_name) AS "r"
FROM social.users_bench AS u
