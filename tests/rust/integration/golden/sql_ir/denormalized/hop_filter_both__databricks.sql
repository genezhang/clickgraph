SELECT 
      t0.origin_code AS `a.code`, 
      t0.dest_code AS `b.code`
FROM db_denormalized.flights_denorm AS t0
WHERE (t0.origin_state = 'CA' AND t0.dest_state = 'NY')
