SELECT 
      a.city AS "a.city", 
      a.country AS "a.country", 
      a.email_address AS "a.email", 
      a.is_active AS "a.is_active", 
      a.full_name AS "a.name", 
      a.registration_date AS "a.registration_date", 
      a.user_id AS "a.user_id", 
      b.author_id AS "b.author_id", 
      b.post_content AS "b.content", 
      b.post_date AS "b.date", 
      b.post_id AS "b.post_id", 
      b.post_title AS "b.title", 
      t0.user_id AS "t0.from_id", 
      t0.post_id AS "t0.to_id", 
      t0.authored_date AS "t0.authored_date", 
      tuple('fixed_path', 'a', 'b', 't0') AS "p"
FROM social.users_bench AS a
INNER JOIN social.authored_bench AS t0 ON t0.user_id = a.user_id
INNER JOIN social.posts_bench AS b ON b.post_id = t0.post_id
