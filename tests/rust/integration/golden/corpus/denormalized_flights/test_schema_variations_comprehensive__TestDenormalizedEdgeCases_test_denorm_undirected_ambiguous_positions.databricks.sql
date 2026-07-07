SELECT count(*) AS `flights` FROM (
SELECT 1 AS __dummy
FROM db_denormalized.flights_denorm AS f
WHERE f.origin_city = 'Seattle'
UNION ALL 
SELECT 1 AS __dummy
FROM db_denormalized.flights_denorm AS f
WHERE f.dest_city = 'Seattle'
) AS __union
