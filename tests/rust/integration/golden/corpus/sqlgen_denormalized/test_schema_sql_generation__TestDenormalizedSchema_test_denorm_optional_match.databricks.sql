WITH __denorm_scan_a AS (
SELECT 
      a.origin_city AS `city`, 
      a.origin_code AS `code`, 
      a.origin_state AS `state`
FROM db_denormalized.flights_denorm AS a
UNION DISTINCT 
SELECT 
      a.dest_city AS `city`, 
      a.dest_code AS `code`, 
      a.dest_state AS `state`
FROM db_denormalized.flights_denorm AS a

)
SELECT 
      a.code AS `a.code`, 
      count(*) AS `outgoing`
FROM __denorm_scan_a AS a
LEFT JOIN db_denormalized.flights_denorm AS f ON a.code = f.origin_code
GROUP BY a.code
