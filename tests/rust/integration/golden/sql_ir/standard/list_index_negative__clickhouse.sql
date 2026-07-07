SELECT 
      [10, 20, 30][if((0 - 1) >= 0, (0 - 1)+1, (0 - 1))] AS "last", 
      [10, 20, 30][1] AS "first"
FROM social.users_bench AS u
