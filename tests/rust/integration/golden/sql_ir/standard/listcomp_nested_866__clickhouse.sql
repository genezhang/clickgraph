SELECT 
      arrayMap(x -> arrayMap(y -> y, range(1, (x) + 1)), range(1, (3) + 1)) AS "c"
