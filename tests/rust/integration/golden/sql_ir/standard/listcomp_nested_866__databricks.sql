SELECT 
      transform(sequence(1, 3), x -> transform(sequence(1, x), y -> y)) AS `c`
