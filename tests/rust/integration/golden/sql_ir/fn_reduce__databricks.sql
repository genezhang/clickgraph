SELECT 
      aggregate(array(1, 2, 3), bigint(0), (s, x) -> s + x) AS `r`
FROM social.users_bench AS u
