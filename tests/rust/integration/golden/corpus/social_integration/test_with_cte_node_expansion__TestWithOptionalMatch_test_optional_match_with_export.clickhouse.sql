WITH optional_o1 AS (
SELECT 
      v1.follower_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id", 
      v2.age AS "p2_v2_age", 
      v2.city AS "p2_v2_city", 
      v2.country AS "p2_v2_country", 
      v2.email_address AS "p2_v2_email", 
      v2.is_active AS "p2_v2_is_active", 
      v2.full_name AS "p2_v2_name", 
      v2.registration_date AS "p2_v2_registration_date", 
      v2.user_id AS "p2_v2_user_id"
FROM test_integration.user_follows_test AS v1
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
), 
with_w2 AS (
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
      o1.v2__user_id AS "v4__user_id", 
      o1.p2_v2_age AS "p2_v4_age", 
      o1.p2_v2_city AS "p2_v4_city", 
      o1.p2_v2_country AS "p2_v4_country", 
      o1.p2_v2_email AS "p2_v4_email", 
      o1.p2_v2_is_active AS "p2_v4_is_active", 
      o1.p2_v2_name AS "p2_v4_name", 
      o1.p2_v2_registration_date AS "p2_v4_registration_date", 
      o1.p2_v2_user_id AS "p2_v4_user_id"
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
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
-- column a: Node { label: "User" } [("age", "a.age"), ("city", "a.city"), ("country", "a.country"), ("email", "a.email"), ("is_active", "a.is_active"), ("name", "a.name"), ("registration_date", "a.registration_date"), ("user_id", "a.user_id")]
-- column b: Node { label: "User" } [("age", "b.age"), ("city", "b.city"), ("country", "b.country"), ("email", "b.email"), ("is_active", "b.is_active"), ("name", "b.name"), ("registration_date", "b.registration_date"), ("user_id", "b.user_id")]