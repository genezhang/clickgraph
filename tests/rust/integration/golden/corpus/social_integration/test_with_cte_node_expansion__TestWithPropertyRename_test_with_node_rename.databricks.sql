WITH with_person_cte_0 AS (SELECT 
      a.age AS `p1_a_age`, 
      a.city AS `p1_a_city`, 
      a.country AS `p1_a_country`, 
      a.email_address AS `p1_a_email`, 
      a.is_active AS `p1_a_is_active`, 
      a.full_name AS `p1_a_name`, 
      a.registration_date AS `p1_a_registration_date`, 
      a.user_id AS `p1_a_user_id`
FROM test_integration.users_test AS a
)
SELECT 
      person.p1_a_age AS `person.age`, 
      person.p1_a_city AS `person.city`, 
      person.p1_a_country AS `person.country`, 
      person.p1_a_email AS `person.email`, 
      person.p1_a_email AS `person.email_address`, 
      person.p1_a_name AS `person.full_name`, 
      person.p1_a_is_active AS `person.is_active`, 
      person.p1_a_name AS `person.name`, 
      person.p1_a_registration_date AS `person.registration_date`, 
      person.p1_a_user_id AS `person.user_id`
FROM with_person_cte_0 AS person
LIMIT 1