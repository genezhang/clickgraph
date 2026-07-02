SELECT 
      arrayFold(x, s -> s + x, [1, 2, 3], toInt64(0)) AS "r"
FROM social.users_bench AS u
