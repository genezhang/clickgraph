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
)
SELECT 
      w1.p2_v1_age AS `person.age`, 
      w1.p2_v1_city AS `person.city`, 
      w1.p2_v1_country AS `person.country`, 
      w1.p2_v1_email AS `person.email`, 
      w1.p2_v1_is_active AS `person.is_active`, 
      w1.p2_v1_name AS `person.name`, 
      w1.p2_v1_registration_date AS `person.registration_date`, 
      w1.p2_v1_user_id AS `person.user_id`
FROM with_w1 AS w1
LIMIT 1
-- column person: Node { label: "User" } [("age", "person.age"), ("city", "person.city"), ("country", "person.country"), ("email", "person.email"), ("is_active", "person.is_active"), ("name", "person.name"), ("registration_date", "person.registration_date"), ("user_id", "person.user_id")]