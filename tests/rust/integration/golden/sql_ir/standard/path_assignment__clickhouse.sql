SELECT 
      a.city AS "a.city", 
      a.country AS "a.country", 
      a.email_address AS "a.email", 
      a.is_active AS "a.is_active", 
      a.full_name AS "a.name", 
      a.registration_date AS "a.registration_date", 
      a.user_id AS "a.user_id", 
      b.city AS "b.city", 
      b.country AS "b.country", 
      b.email_address AS "b.email", 
      b.is_active AS "b.is_active", 
      b.full_name AS "b.name", 
      b.registration_date AS "b.registration_date", 
      b.user_id AS "b.user_id", 
      t0.follower_id AS "t0.from_id", 
      t0.followed_id AS "t0.to_id", 
      t0.follow_date AS "t0.follow_date", 
      tuple('fixed_path', 'a', 'b', 't0') AS "p"
FROM social.users_bench AS a
INNER JOIN social.user_follows_bench AS t0 ON t0.follower_id = a.user_id
INNER JOIN social.users_bench AS b ON b.user_id = t0.followed_id
