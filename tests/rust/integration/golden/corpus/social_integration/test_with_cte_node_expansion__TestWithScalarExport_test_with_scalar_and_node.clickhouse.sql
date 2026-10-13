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
      count(v2.user_id) AS "v4"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
GROUP BY v0.user_id, v0.age, v0.city, v0.country, v0.email_address, v0.is_active, v0.full_name, v0.registration_date, v0.user_id
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
      w1.v4 AS "follower_count"
FROM with_w1 AS w1
LIMIT 1