SELECT 
      t0.origin_city AS `t1.city`, 
      t0.origin_code AS `t1.code`, 
      t0.origin_state AS `t1.state`, 
      t0.dest_city AS `t2.city`, 
      t0.dest_code AS `t2.code`, 
      t0.dest_state AS `t2.state`, 
      t0.arrival_time AS `t0.arrival_time`, 
      t0.carrier AS `t0.carrier`, 
      t0.departure_time AS `t0.departure_time`, 
      t0.distance AS `t0.distance`, 
      t0.flight_id AS `t0.flight_id`, 
      t0.flight_number AS `t0.flight_num`, 
      struct('fixed_path', 't1', 't2', 't0') AS `p`
FROM db_denormalized.flights_denorm AS t0
LIMIT 10