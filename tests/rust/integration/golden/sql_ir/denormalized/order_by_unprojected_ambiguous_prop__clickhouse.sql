SELECT `n.city` AS `n.city` FROM (
SELECT 
      n.origin_city AS "n.city", 
      n.origin_state AS "__order_col_0"
FROM db_denormalized.flights_denorm AS n
UNION DISTINCT 
SELECT 
      n.dest_city AS "n.city", 
      n.dest_state AS "__order_col_0"
FROM db_denormalized.flights_denorm AS n
) AS __union
ORDER BY __union.`__order_col_0` ASC
