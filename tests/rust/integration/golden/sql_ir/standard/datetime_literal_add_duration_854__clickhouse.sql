SELECT 
      parseDateTime64BestEffort('2024-06-15', 3) + toIntervalDay(7) AS "d"
