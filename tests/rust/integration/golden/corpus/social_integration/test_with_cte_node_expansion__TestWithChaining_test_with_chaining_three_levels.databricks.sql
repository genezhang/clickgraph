WITH with_w1 AS (
SELECT 
      v0.user_id AS `v1__user_id`, 
      v0.age AS `p2_v1_age`, 
      v0.city AS `p2_v1_city`, 
      v0.country AS `p2_v1_country`, 
      v0.email_address AS `p2_v1_email`, 
      v0.is_active AS `p2_v1_is_active`, 
      v0.full_name AS `p2_v1_name`, 
      v0.registration_date AS `p2_v1_registration_date`, 
      v0.user_id AS `p2_v1_user_id`
FROM test_integration.users_test AS v0
), 
with_w2 AS (
SELECT 
      w1.v1__user_id AS `v3__user_id`, 
      w1.p2_v1_age AS `p2_v3_age`, 
      w1.p2_v1_city AS `p2_v3_city`, 
      w1.p2_v1_country AS `p2_v3_country`, 
      w1.p2_v1_email AS `p2_v3_email`, 
      w1.p2_v1_is_active AS `p2_v3_is_active`, 
      w1.p2_v1_name AS `p2_v3_name`, 
      w1.p2_v1_registration_date AS `p2_v3_registration_date`, 
      w1.p2_v1_user_id AS `p2_v3_user_id`, 
      v2.user_id AS `v4__user_id`, 
      v2.age AS `p2_v4_age`, 
      v2.city AS `p2_v4_city`, 
      v2.country AS `p2_v4_country`, 
      v2.email_address AS `p2_v4_email`, 
      v2.is_active AS `p2_v4_is_active`, 
      v2.full_name AS `p2_v4_name`, 
      v2.registration_date AS `p2_v4_registration_date`, 
      v2.user_id AS `p2_v4_user_id`
FROM with_w1 AS w1
CROSS JOIN test_integration.users_test AS v2
), 
with_w3 AS (
SELECT 
      w2.v3__user_id AS `v6__user_id`, 
      w2.p2_v3_age AS `p2_v6_age`, 
      w2.p2_v3_city AS `p2_v6_city`, 
      w2.p2_v3_country AS `p2_v6_country`, 
      w2.p2_v3_email AS `p2_v6_email`, 
      w2.p2_v3_is_active AS `p2_v6_is_active`, 
      w2.p2_v3_name AS `p2_v6_name`, 
      w2.p2_v3_registration_date AS `p2_v6_registration_date`, 
      w2.p2_v3_user_id AS `p2_v6_user_id`, 
      w2.v4__user_id AS `v7__user_id`, 
      w2.p2_v4_age AS `p2_v7_age`, 
      w2.p2_v4_city AS `p2_v7_city`, 
      w2.p2_v4_country AS `p2_v7_country`, 
      w2.p2_v4_email AS `p2_v7_email`, 
      w2.p2_v4_is_active AS `p2_v7_is_active`, 
      w2.p2_v4_name AS `p2_v7_name`, 
      w2.p2_v4_registration_date AS `p2_v7_registration_date`, 
      w2.p2_v4_user_id AS `p2_v7_user_id`, 
      v5.user_id AS `v8__user_id`, 
      v5.age AS `p2_v8_age`, 
      v5.city AS `p2_v8_city`, 
      v5.country AS `p2_v8_country`, 
      v5.email_address AS `p2_v8_email`, 
      v5.is_active AS `p2_v8_is_active`, 
      v5.full_name AS `p2_v8_name`, 
      v5.registration_date AS `p2_v8_registration_date`, 
      v5.user_id AS `p2_v8_user_id`
FROM with_w2 AS w2
CROSS JOIN test_integration.users_test AS v5
)
SELECT 
      w3.p2_v6_age AS `a.age`, 
      w3.p2_v6_city AS `a.city`, 
      w3.p2_v6_country AS `a.country`, 
      w3.p2_v6_email AS `a.email`, 
      w3.p2_v6_is_active AS `a.is_active`, 
      w3.p2_v6_name AS `a.name`, 
      w3.p2_v6_registration_date AS `a.registration_date`, 
      w3.p2_v6_user_id AS `a.user_id`, 
      w3.p2_v7_age AS `b.age`, 
      w3.p2_v7_city AS `b.city`, 
      w3.p2_v7_country AS `b.country`, 
      w3.p2_v7_email AS `b.email`, 
      w3.p2_v7_is_active AS `b.is_active`, 
      w3.p2_v7_name AS `b.name`, 
      w3.p2_v7_registration_date AS `b.registration_date`, 
      w3.p2_v7_user_id AS `b.user_id`, 
      w3.p2_v8_age AS `c.age`, 
      w3.p2_v8_city AS `c.city`, 
      w3.p2_v8_country AS `c.country`, 
      w3.p2_v8_email AS `c.email`, 
      w3.p2_v8_is_active AS `c.is_active`, 
      w3.p2_v8_name AS `c.name`, 
      w3.p2_v8_registration_date AS `c.registration_date`, 
      w3.p2_v8_user_id AS `c.user_id`
FROM with_w3 AS w3
LIMIT 1
-- column a: Node { label: "User" } [("age", "a.age"), ("city", "a.city"), ("country", "a.country"), ("email", "a.email"), ("is_active", "a.is_active"), ("name", "a.name"), ("registration_date", "a.registration_date"), ("user_id", "a.user_id")]
-- column b: Node { label: "User" } [("age", "b.age"), ("city", "b.city"), ("country", "b.country"), ("email", "b.email"), ("is_active", "b.is_active"), ("name", "b.name"), ("registration_date", "b.registration_date"), ("user_id", "b.user_id")]
-- column c: Node { label: "User" } [("age", "c.age"), ("city", "c.city"), ("country", "c.country"), ("email", "c.email"), ("is_active", "c.is_active"), ("name", "c.name"), ("registration_date", "c.registration_date"), ("user_id", "c.user_id")]