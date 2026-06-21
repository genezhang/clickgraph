SELECT 
      slice(array(10, 20, 30), 2, greatest(size(array(10, 20, 30)) - 1, 0)) AS `t`
FROM social.users_bench AS u
