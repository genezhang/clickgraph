WITH with_w1 AS (
SELECT 
      v0.user_id AS "v5__user_id", 
      v0.age AS "p2_v5_age", 
      v0.city AS "p2_v5_city", 
      v0.country AS "p2_v5_country", 
      v0.email_address AS "p2_v5_email", 
      v0.is_active AS "p2_v5_is_active", 
      v0.full_name AS "p2_v5_name", 
      v0.registration_date AS "p2_v5_registration_date", 
      v0.user_id AS "p2_v5_user_id", 
      v2.user_id AS "v6__user_id", 
      v2.age AS "p2_v6_age", 
      v2.city AS "p2_v6_city", 
      v2.country AS "p2_v6_country", 
      v2.email_address AS "p2_v6_email", 
      v2.is_active AS "p2_v6_is_active", 
      v2.full_name AS "p2_v6_name", 
      v2.registration_date AS "p2_v6_registration_date", 
      v2.user_id AS "p2_v6_user_id", 
      v4.user_id AS "v7__user_id", 
      v4.age AS "p2_v7_age", 
      v4.city AS "p2_v7_city", 
      v4.country AS "p2_v7_country", 
      v4.email_address AS "p2_v7_email", 
      v4.is_active AS "p2_v7_is_active", 
      v4.full_name AS "p2_v7_name", 
      v4.registration_date AS "p2_v7_registration_date", 
      v4.user_id AS "p2_v7_user_id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.user_follows_test AS v3 ON v3.follower_id = v2.user_id
JOIN test_integration.users_test AS v4 ON v3.followed_id = v4.user_id
WHERE v1.follow_id <> v3.follow_id
)
SELECT 
      w1.p2_v5_age AS "a.age", 
      w1.p2_v5_city AS "a.city", 
      w1.p2_v5_country AS "a.country", 
      w1.p2_v5_email AS "a.email", 
      w1.p2_v5_is_active AS "a.is_active", 
      w1.p2_v5_name AS "a.name", 
      w1.p2_v5_registration_date AS "a.registration_date", 
      w1.p2_v5_user_id AS "a.user_id", 
      w1.p2_v6_age AS "b.age", 
      w1.p2_v6_city AS "b.city", 
      w1.p2_v6_country AS "b.country", 
      w1.p2_v6_email AS "b.email", 
      w1.p2_v6_is_active AS "b.is_active", 
      w1.p2_v6_name AS "b.name", 
      w1.p2_v6_registration_date AS "b.registration_date", 
      w1.p2_v6_user_id AS "b.user_id", 
      w1.p2_v7_age AS "c.age", 
      w1.p2_v7_city AS "c.city", 
      w1.p2_v7_country AS "c.country", 
      w1.p2_v7_email AS "c.email", 
      w1.p2_v7_is_active AS "c.is_active", 
      w1.p2_v7_name AS "c.name", 
      w1.p2_v7_registration_date AS "c.registration_date", 
      w1.p2_v7_user_id AS "c.user_id"
FROM with_w1 AS w1
LIMIT 1
-- column a: Node { label: "User" } [("age", "a.age"), ("city", "a.city"), ("country", "a.country"), ("email", "a.email"), ("is_active", "a.is_active"), ("name", "a.name"), ("registration_date", "a.registration_date"), ("user_id", "a.user_id")]
-- column b: Node { label: "User" } [("age", "b.age"), ("city", "b.city"), ("country", "b.country"), ("email", "b.email"), ("is_active", "b.is_active"), ("name", "b.name"), ("registration_date", "b.registration_date"), ("user_id", "b.user_id")]
-- column c: Node { label: "User" } [("age", "c.age"), ("city", "c.city"), ("country", "c.country"), ("email", "c.email"), ("is_active", "c.is_active"), ("name", "c.name"), ("registration_date", "c.registration_date"), ("user_id", "c.user_id")]