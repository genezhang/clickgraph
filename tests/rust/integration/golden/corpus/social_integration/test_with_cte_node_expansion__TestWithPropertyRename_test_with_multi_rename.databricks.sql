WITH with_w1 AS (
SELECT 
      v0.user_id AS `v3__user_id`, 
      v0.age AS `p2_v3_age`, 
      v0.city AS `p2_v3_city`, 
      v0.country AS `p2_v3_country`, 
      v0.email_address AS `p2_v3_email`, 
      v0.is_active AS `p2_v3_is_active`, 
      v0.full_name AS `p2_v3_name`, 
      v0.registration_date AS `p2_v3_registration_date`, 
      v0.user_id AS `p2_v3_user_id`, 
      v2.user_id AS `v4__user_id`, 
      v2.age AS `p2_v4_age`, 
      v2.city AS `p2_v4_city`, 
      v2.country AS `p2_v4_country`, 
      v2.email_address AS `p2_v4_email`, 
      v2.is_active AS `p2_v4_is_active`, 
      v2.full_name AS `p2_v4_name`, 
      v2.registration_date AS `p2_v4_registration_date`, 
      v2.user_id AS `p2_v4_user_id`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
)
SELECT 
      w1.p2_v3_age AS `follower.age`, 
      w1.p2_v3_city AS `follower.city`, 
      w1.p2_v3_country AS `follower.country`, 
      w1.p2_v3_email AS `follower.email`, 
      w1.p2_v3_is_active AS `follower.is_active`, 
      w1.p2_v3_name AS `follower.name`, 
      w1.p2_v3_registration_date AS `follower.registration_date`, 
      w1.p2_v3_user_id AS `follower.user_id`, 
      w1.p2_v4_age AS `followed.age`, 
      w1.p2_v4_city AS `followed.city`, 
      w1.p2_v4_country AS `followed.country`, 
      w1.p2_v4_email AS `followed.email`, 
      w1.p2_v4_is_active AS `followed.is_active`, 
      w1.p2_v4_name AS `followed.name`, 
      w1.p2_v4_registration_date AS `followed.registration_date`, 
      w1.p2_v4_user_id AS `followed.user_id`
FROM with_w1 AS w1
LIMIT 1