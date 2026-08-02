SELECT 
      parseDateTime64BestEffort('2024-06-15', 3) + make_dt_interval(7, 0, 0, 0) AS `d`
