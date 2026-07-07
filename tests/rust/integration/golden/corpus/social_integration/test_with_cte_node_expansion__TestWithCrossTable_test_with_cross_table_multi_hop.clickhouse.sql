SELECT 
      a.age AS "a.age", 
      a.city AS "a.city", 
      a.country AS "a.country", 
      a.email_address AS "a.email", 
      a.is_active AS "a.is_active", 
      a.full_name AS "a.name", 
      a.registration_date AS "a.registration_date", 
      a.user_id AS "a.user_id", 
      b.age AS "b.age", 
      b.city AS "b.city", 
      b.country AS "b.country", 
      b.email_address AS "b.email", 
      b.is_active AS "b.is_active", 
      b.full_name AS "b.name", 
      b.registration_date AS "b.registration_date", 
      b.user_id AS "b.user_id", 
      c.author_id AS "c.author_id", 
      c.post_content AS "c.content", 
      c.post_date AS "c.created_at", 
      c.post_id AS "c.post_id", 
      c.post_title AS "c.title"
FROM test_integration.users_test AS a
INNER JOIN test_integration.posts_test AS c ON c.author_id = a.user_id
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users_test AS b ON b.user_id = t0.followed_id
INNER JOIN test_integration.posts_test AS t1 ON t1.author_id = a.user_id
LIMIT 1