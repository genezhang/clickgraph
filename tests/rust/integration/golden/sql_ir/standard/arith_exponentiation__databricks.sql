SELECT 
      POWER(2, 3) AS `a`, 
      1 + POWER(2, 3) AS `b`, 
      POWER(2, 3) * 4 AS `c`, 
      POWER(POWER(2, 3), 2) AS `d`, 
      POWER(0 - 2, 2) AS `e`
FROM social.users_bench AS u
