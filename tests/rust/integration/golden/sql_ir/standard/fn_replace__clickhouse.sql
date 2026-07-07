SELECT 
      replaceAll(u.full_name, 'a', 'X') AS "r"
FROM social.users_bench AS u
