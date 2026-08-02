SELECT 
      arraySlice([10, 20, 30, 40, 50], if((0 - 2) >= 0, (0 - 2), greatest(length([10, 20, 30, 40, 50]) + (0 - 2), 0)) + 1) AS "a", 
      arraySlice([10, 20, 30, 40, 50], 1 + 1, greatest(if((0 - 1) >= 0, (0 - 1), greatest(length([10, 20, 30, 40, 50]) + (0 - 1), 0)) - 1, 0)) AS "b", 
      arraySlice([10, 20, 30, 40, 50], if((0 - 10) >= 0, (0 - 10), greatest(length([10, 20, 30, 40, 50]) + (0 - 10), 0)) + 1, greatest(2 - if((0 - 10) >= 0, (0 - 10), greatest(length([10, 20, 30, 40, 50]) + (0 - 10), 0)), 0)) AS "c", 
      arraySlice([10, 20, 30, 40, 50], if((0 - 3) >= 0, (0 - 3), greatest(length([10, 20, 30, 40, 50]) + (0 - 3), 0)) + 1, greatest(if((0 - 1) >= 0, (0 - 1), greatest(length([10, 20, 30, 40, 50]) + (0 - 1), 0)) - if((0 - 3) >= 0, (0 - 3), greatest(length([10, 20, 30, 40, 50]) + (0 - 3), 0)), 0)) AS "d", 
      arraySlice([10, 20, 30, 40, 50], 1, if((0 - 1) >= 0, (0 - 1), greatest(length([10, 20, 30, 40, 50]) + (0 - 1), 0))) AS "e"
FROM social.users_bench AS u
