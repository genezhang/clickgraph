SELECT 
      slice(array(10, 20, 30, 40), 3 + 1, greatest(1 - 3, 0)) AS `s`
FROM social.users_bench AS u
