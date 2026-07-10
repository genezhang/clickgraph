SELECT 
      t0.origin_code AS `a.code`, 
      t1.origin_code AS `b.code`, 
      t1.dest_code AS `c.code`
FROM db_denormalized.flights_denorm AS t0
LEFT JOIN db_denormalized.flights_denorm AS t1 ON t1.origin_code = t0.dest_code
