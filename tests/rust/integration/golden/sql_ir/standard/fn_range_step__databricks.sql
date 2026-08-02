SELECT 
      sequence(1, 10, 2) AS `asc`, 
      sequence(5, 1, 0 - 1) AS `desc`
FROM social.users_bench AS u
