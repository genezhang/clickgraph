SELECT 
      arrayMap(x -> x * 10, arrayFilter(x -> x % 2 = 0, range(1, (5) + 1))) AS "c"
