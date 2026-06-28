SELECT 
      arraySlice([10, 20, 30, 40], 1 + 1, greatest(3 - 1, 0)) AS "s"
FROM social.users_bench AS u
