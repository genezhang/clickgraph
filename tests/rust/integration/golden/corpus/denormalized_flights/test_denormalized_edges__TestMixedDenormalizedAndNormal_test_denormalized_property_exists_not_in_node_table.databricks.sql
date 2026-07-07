SELECT 
      count(*) AS `count`
FROM db_denormalized.flights_denorm AS f
WHERE f.origin_city = 'Los Angeles'
