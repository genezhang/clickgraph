WITH with_w1 AS (
SELECT 
      v0.user_id AS "v1__user_id", 
      v0.age AS "p2_v1_age", 
      v0.city AS "p2_v1_city", 
      v0.country AS "p2_v1_country", 
      v0.email_address AS "p2_v1_email", 
      v0.is_active AS "p2_v1_is_active", 
      v0.full_name AS "p2_v1_name", 
      v0.registration_date AS "p2_v1_registration_date", 
      v0.user_id AS "p2_v1_user_id"
FROM test_integration.users_test AS v0
), 
with_w2 AS (
SELECT 
      w1.v1__user_id AS "v3__user_id", 
      w1.p2_v1_age AS "p2_v3_age", 
      w1.p2_v1_city AS "p2_v3_city", 
      w1.p2_v1_country AS "p2_v3_country", 
      w1.p2_v1_email AS "p2_v3_email", 
      w1.p2_v1_is_active AS "p2_v3_is_active", 
      w1.p2_v1_name AS "p2_v3_name", 
      w1.p2_v1_registration_date AS "p2_v3_registration_date", 
      w1.p2_v1_user_id AS "p2_v3_user_id", 
      v2.user_id AS "v4__user_id", 
      v2.age AS "p2_v4_age", 
      v2.city AS "p2_v4_city", 
      v2.country AS "p2_v4_country", 
      v2.email_address AS "p2_v4_email", 
      v2.is_active AS "p2_v4_is_active", 
      v2.full_name AS "p2_v4_name", 
      v2.registration_date AS "p2_v4_registration_date", 
      v2.user_id AS "p2_v4_user_id"
FROM with_w1 AS w1
CROSS JOIN test_integration.users_test AS v2
)
SELECT 
      w2.p2_v3_age AS "a.age", 
      w2.p2_v3_city AS "a.city", 
      w2.p2_v3_country AS "a.country", 
      w2.p2_v3_email AS "a.email", 
      w2.p2_v3_is_active AS "a.is_active", 
      w2.p2_v3_name AS "a.name", 
      w2.p2_v3_registration_date AS "a.registration_date", 
      w2.p2_v3_user_id AS "a.user_id", 
      w2.p2_v4_age AS "b.age", 
      w2.p2_v4_city AS "b.city", 
      w2.p2_v4_country AS "b.country", 
      w2.p2_v4_email AS "b.email", 
      w2.p2_v4_is_active AS "b.is_active", 
      w2.p2_v4_name AS "b.name", 
      w2.p2_v4_registration_date AS "b.registration_date", 
      w2.p2_v4_user_id AS "b.user_id"
FROM with_w2 AS w2
LIMIT 1