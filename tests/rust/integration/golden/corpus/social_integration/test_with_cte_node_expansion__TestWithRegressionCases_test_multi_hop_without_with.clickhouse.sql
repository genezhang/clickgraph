SELECT 
      v0.age AS "a.age", 
      v0.city AS "a.city", 
      v0.country AS "a.country", 
      v0.email_address AS "a.email", 
      v0.is_active AS "a.is_active", 
      v0.full_name AS "a.name", 
      v0.registration_date AS "a.registration_date", 
      v0.user_id AS "a.user_id", 
      v2.age AS "b.age", 
      v2.city AS "b.city", 
      v2.country AS "b.country", 
      v2.email_address AS "b.email", 
      v2.is_active AS "b.is_active", 
      v2.full_name AS "b.name", 
      v2.registration_date AS "b.registration_date", 
      v2.user_id AS "b.user_id", 
      v4.age AS "c.age", 
      v4.city AS "c.city", 
      v4.country AS "c.country", 
      v4.email_address AS "c.email", 
      v4.is_active AS "c.is_active", 
      v4.full_name AS "c.name", 
      v4.registration_date AS "c.registration_date", 
      v4.user_id AS "c.user_id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.user_follows_test AS v3 ON v3.follower_id = v2.user_id
JOIN test_integration.users_test AS v4 ON v3.followed_id = v4.user_id
WHERE v1.follow_id <> v3.follow_id
LIMIT 1