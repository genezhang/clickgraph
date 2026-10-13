WITH with_w1 AS (
SELECT 
      v0.user_id AS "v3__user_id", 
      v0.age AS "p2_v3_age", 
      v0.city AS "p2_v3_city", 
      v0.country AS "p2_v3_country", 
      v0.email_address AS "p2_v3_email", 
      v0.is_active AS "p2_v3_is_active", 
      v0.full_name AS "p2_v3_name", 
      v0.registration_date AS "p2_v3_registration_date", 
      v0.user_id AS "p2_v3_user_id", 
      v2.user_id AS "v4__user_id", 
      v2.age AS "p2_v4_age", 
      v2.city AS "p2_v4_city", 
      v2.country AS "p2_v4_country", 
      v2.email_address AS "p2_v4_email", 
      v2.is_active AS "p2_v4_is_active", 
      v2.full_name AS "p2_v4_name", 
      v2.registration_date AS "p2_v4_registration_date", 
      v2.user_id AS "p2_v4_user_id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
)
SELECT 
      w1.p2_v3_age AS "a.age", 
      w1.p2_v3_city AS "a.city", 
      w1.p2_v3_country AS "a.country", 
      w1.p2_v3_email AS "a.email", 
      w1.p2_v3_is_active AS "a.is_active", 
      w1.p2_v3_name AS "a.name", 
      w1.p2_v3_registration_date AS "a.registration_date", 
      w1.p2_v3_user_id AS "a.user_id", 
      w1.p2_v4_age AS "b.age", 
      w1.p2_v4_city AS "b.city", 
      w1.p2_v4_country AS "b.country", 
      w1.p2_v4_email AS "b.email", 
      w1.p2_v4_is_active AS "b.is_active", 
      w1.p2_v4_name AS "b.name", 
      w1.p2_v4_registration_date AS "b.registration_date", 
      w1.p2_v4_user_id AS "b.user_id", 
      v6.author_id AS "c.author_id", 
      v6.post_content AS "c.content", 
      v6.post_date AS "c.created_at", 
      v6.post_id AS "c.post_id", 
      v6.post_title AS "c.title"
FROM with_w1 AS w1
JOIN test_integration.posts_test AS v5 ON v5.author_id = w1.v3__user_id
JOIN test_integration.posts_test AS v6 ON v5.post_id = v6.post_id
LIMIT 1