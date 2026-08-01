SELECT 
      arrayElementOrNull([10, 20, 30], 11) AS "oob_hi", 
      arrayElementOrNull([10, 20, 30], if((0 - 10) >= 0, (0 - 10)+1, (0 - 10))) AS "oob_lo", 
      arrayElementOrNull([10, 20, 30], 1) AS "first"
FROM social.users_bench AS u
