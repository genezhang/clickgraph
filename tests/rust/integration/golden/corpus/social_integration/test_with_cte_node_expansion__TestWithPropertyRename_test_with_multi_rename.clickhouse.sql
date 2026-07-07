WITH with_followed_follower_cte_0 AS (SELECT 
      a.age AS "p1_a_age", 
      a.city AS "p1_a_city", 
      a.country AS "p1_a_country", 
      a.email_address AS "p1_a_email", 
      a.is_active AS "p1_a_is_active", 
      a.full_name AS "p1_a_name", 
      a.registration_date AS "p1_a_registration_date", 
      a.user_id AS "p1_a_user_id", 
      b.age AS "p1_b_age", 
      b.city AS "p1_b_city", 
      b.country AS "p1_b_country", 
      b.email_address AS "p1_b_email", 
      b.is_active AS "p1_b_is_active", 
      b.full_name AS "p1_b_name", 
      b.registration_date AS "p1_b_registration_date", 
      b.user_id AS "p1_b_user_id"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users_test AS b ON b.user_id = t0.followed_id
)
SELECT 
      followed_follower.p1_a_age AS "follower.age", 
      followed_follower.p1_a_city AS "follower.city", 
      followed_follower.p1_a_country AS "follower.country", 
      followed_follower.p1_a_email AS "follower.email", 
      followed_follower.p1_a_email AS "follower.email_address", 
      followed_follower.p1_a_name AS "follower.full_name", 
      followed_follower.p1_a_is_active AS "follower.is_active", 
      followed_follower.p1_a_name AS "follower.name", 
      followed_follower.p1_a_registration_date AS "follower.registration_date", 
      followed_follower.p1_a_user_id AS "follower.user_id", 
      followed_follower.p1_b_age AS "followed.age", 
      followed_follower.p1_b_city AS "followed.city", 
      followed_follower.p1_b_country AS "followed.country", 
      followed_follower.p1_b_email AS "followed.email", 
      followed_follower.p1_b_email AS "followed.email_address", 
      followed_follower.p1_b_name AS "followed.full_name", 
      followed_follower.p1_b_is_active AS "followed.is_active", 
      followed_follower.p1_b_name AS "followed.name", 
      followed_follower.p1_b_registration_date AS "followed.registration_date", 
      followed_follower.p1_b_user_id AS "followed.user_id"
FROM with_followed_follower_cte_0 AS followed_follower
LIMIT 1