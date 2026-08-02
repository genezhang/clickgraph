SELECT 
      range(1, (10) + if((2) < 0, -1, 1), 2) AS "asc", 
      range(5, (1) + if((0 - 1) < 0, -1, 1), 0 - 1) AS "desc"
FROM social.users_bench AS u
