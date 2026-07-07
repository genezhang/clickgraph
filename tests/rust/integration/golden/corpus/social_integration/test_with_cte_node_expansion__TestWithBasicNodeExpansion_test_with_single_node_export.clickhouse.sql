WITH with_a_cte_0 AS (SELECT 
      a.age AS "p1_a_age", 
      a.city AS "p1_a_city", 
      a.country AS "p1_a_country", 
      a.email_address AS "p1_a_email", 
      a.is_active AS "p1_a_is_active", 
      a.full_name AS "p1_a_name", 
      a.registration_date AS "p1_a_registration_date", 
      a.user_id AS "p1_a_user_id"
FROM test_integration.users_test AS a
)
SELECT 
      a.p1_a_age AS "a.age", 
      a.p1_a_city AS "a.city", 
      a.p1_a_country AS "a.country", 
      a.p1_a_email AS "a.email", 
      a.p1_a_email AS "a.email_address", 
      a.p1_a_name AS "a.full_name", 
      a.p1_a_is_active AS "a.is_active", 
      a.p1_a_name AS "a.name", 
      a.p1_a_registration_date AS "a.registration_date", 
      a.p1_a_user_id AS "a.user_id"
FROM with_a_cte_0 AS a
LIMIT 1