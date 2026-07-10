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
      b.user_id AS "b.user_id"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS r ON r.follower_id = a.user_id
INNER JOIN test_integration.users_test AS b ON b.user_id = r.followed_id
LIMIT 1