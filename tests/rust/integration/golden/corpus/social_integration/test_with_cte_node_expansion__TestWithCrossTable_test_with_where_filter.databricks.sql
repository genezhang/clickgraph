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
WHERE v0.user_id > 1
)
SELECT 
      w1.p2_v1_age AS `a.age`, 
      w1.p2_v1_city AS `a.city`, 
      w1.p2_v1_country AS `a.country`, 
      w1.p2_v1_email AS `a.email`, 
      w1.p2_v1_is_active AS `a.is_active`, 
      w1.p2_v1_name AS `a.name`, 
      w1.p2_v1_registration_date AS `a.registration_date`, 
      w1.p2_v1_user_id AS `a.user_id`
FROM with_w1 AS w1

-- column a: Node { label: "User" } [("age", "a.age"), ("city", "a.city"), ("country", "a.country"), ("email", "a.email"), ("is_active", "a.is_active"), ("name", "a.name"), ("registration_date", "a.registration_date"), ("user_id", "a.user_id")]