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
      t0.dest_code AS `b.code`, 
      t1.dest_code AS `c.code`
FROM __denorm_scan_a AS a
LEFT JOIN db_denormalized.flights_denorm AS t0 ON a.code = t0.origin_code
LEFT JOIN db_denormalized.flights_denorm AS t1 ON t1.origin_code = t0.dest_code
