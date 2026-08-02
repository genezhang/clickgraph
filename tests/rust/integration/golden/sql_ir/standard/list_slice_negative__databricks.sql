SELECT 
      slice(array(10, 20, 30, 40, 50), if((0 - 2) >= 0, (0 - 2), greatest(size(array(10, 20, 30, 40, 50)) + (0 - 2), 0)) + 1, greatest(size(array(10, 20, 30, 40, 50)) - (if((0 - 2) >= 0, (0 - 2), greatest(size(array(10, 20, 30, 40, 50)) + (0 - 2), 0)) + 1) + 1, 0)) AS `a`, 
      slice(array(10, 20, 30, 40, 50), 1 + 1, greatest(if((0 - 1) >= 0, (0 - 1), greatest(size(array(10, 20, 30, 40, 50)) + (0 - 1), 0)) - 1, 0)) AS `b`, 
      slice(array(10, 20, 30, 40, 50), if((0 - 10) >= 0, (0 - 10), greatest(size(array(10, 20, 30, 40, 50)) + (0 - 10), 0)) + 1, greatest(2 - if((0 - 10) >= 0, (0 - 10), greatest(size(array(10, 20, 30, 40, 50)) + (0 - 10), 0)), 0)) AS `c`, 
      slice(array(10, 20, 30, 40, 50), if((0 - 3) >= 0, (0 - 3), greatest(size(array(10, 20, 30, 40, 50)) + (0 - 3), 0)) + 1, greatest(if((0 - 1) >= 0, (0 - 1), greatest(size(array(10, 20, 30, 40, 50)) + (0 - 1), 0)) - if((0 - 3) >= 0, (0 - 3), greatest(size(array(10, 20, 30, 40, 50)) + (0 - 3), 0)), 0)) AS `d`, 
      slice(array(10, 20, 30, 40, 50), 1, if((0 - 1) >= 0, (0 - 1), greatest(size(array(10, 20, 30, 40, 50)) + (0 - 1), 0))) AS `e`
FROM social.users_bench AS u
