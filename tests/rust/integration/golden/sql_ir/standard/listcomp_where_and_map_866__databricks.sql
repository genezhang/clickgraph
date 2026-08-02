SELECT 
      transform(filter(sequence(1, 5), x -> x % 2 = 0), x -> x * 10) AS `c`
