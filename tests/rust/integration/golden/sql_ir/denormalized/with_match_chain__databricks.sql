WITH with_a_cte_0 AS (SELECT DISTINCT 
      a.origin_city AS `p1_a_city`, 
      a.origin_code AS `p1_a_code`, 
      a.origin_state AS `p1_a_state`
FROM db_denormalized.flights_denorm AS a
WHERE a.origin_state = 'CA'
UNION DISTINCT 
SELECT DISTINCT 
      a.dest_city AS `p1_a_city`, 
      a.dest_code AS `p1_a_code`, 
      a.dest_state AS `p1_a_state`
FROM db_denormalized.flights_denorm AS a
WHERE a.origin_state = 'CA'
)
SELECT 
      a.p1_a_code AS `a.code`, 
      t0.dest_code AS `b.code`
FROM db_denormalized.flights_denorm AS t0
INNER JOIN with_a_cte_0 AS a ON t0.origin_code = a.p1_a_code
