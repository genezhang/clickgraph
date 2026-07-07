SELECT 
      u.age AS `u.age`, 
      u.city AS `u.city`, 
      u.country AS `u.country`, 
      u.email_address AS `u.email`, 
      u.is_active AS `u.is_active`, 
      u.full_name AS `u.name`, 
      u.registration_date AS `u.registration_date`, 
      u.user_id AS `u.user_id`, 
      f.age AS `f.age`, 
      f.city AS `f.city`, 
      f.country AS `f.country`, 
      f.email_address AS `f.email`, 
      f.is_active AS `f.is_active`, 
      f.full_name AS `f.name`, 
      f.registration_date AS `f.registration_date`, 
      f.user_id AS `f.user_id`, 
      r.follower_id AS `r.from_id`, 
      r.followed_id AS `r.to_id`, 
      r.follow_date AS `r.follow_date`, 
      struct('fixed_path', 'u', 'f', 'r') AS `p`
FROM test_integration.users_test AS u
INNER JOIN test_integration.user_follows_test AS r ON r.follower_id = u.user_id
INNER JOIN test_integration.users_test AS f ON f.user_id = r.followed_id
LIMIT 5