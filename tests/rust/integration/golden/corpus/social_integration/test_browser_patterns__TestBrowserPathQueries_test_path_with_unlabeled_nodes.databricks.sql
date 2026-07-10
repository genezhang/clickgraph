SELECT 
      t0.age AS `t0.age`, 
      t0.city AS `t0.city`, 
      t0.country AS `t0.country`, 
      t0.email_address AS `t0.email`, 
      t0.is_active AS `t0.is_active`, 
      t0.full_name AS `t0.name`, 
      t0.registration_date AS `t0.registration_date`, 
      t0.user_id AS `t0.user_id`, 
      t1.age AS `t1.age`, 
      t1.city AS `t1.city`, 
      t1.country AS `t1.country`, 
      t1.email_address AS `t1.email`, 
      t1.is_active AS `t1.is_active`, 
      t1.full_name AS `t1.name`, 
      t1.registration_date AS `t1.registration_date`, 
      t1.user_id AS `t1.user_id`, 
      r.follower_id AS `r.from_id`, 
      r.followed_id AS `r.to_id`, 
      r.follow_date AS `r.follow_date`, 
      struct('fixed_path', 't0', 't1', 'r') AS `p`
FROM test_integration.users_test AS t0
INNER JOIN test_integration.user_follows_test AS r ON r.follower_id = t0.user_id
INNER JOIN test_integration.users_test AS t1 ON t1.user_id = r.followed_id
LIMIT 5