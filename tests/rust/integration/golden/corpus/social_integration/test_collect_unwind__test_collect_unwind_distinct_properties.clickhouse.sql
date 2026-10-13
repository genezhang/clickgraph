WITH with_w1 AS (
SELECT 
      v0.user_id AS "v1__user_id", 
      groupArrayIf(tuple(v0.user_id, v0.age, v0.city, v0.country, v0.email_address, v0.is_active, v0.full_name, v0.registration_date, v0.user_id), NOT isNull(v0.user_id)) AS "v2"
FROM test_integration.users_test AS v0
GROUP BY v0.user_id
), 
with_w2 AS (
SELECT 
      w1.v1__user_id AS "v1__user_id", 
      w1.v2 AS "v2", 
      __cg_element AS "v3"
FROM with_w1 AS w1
ARRAY JOIN w1.v2 AS __cg_element
)
SELECT DISTINCT 
      tupleElement(w2.v3, 3) AS "user.city"
FROM with_w2 AS w2
ORDER BY tupleElement(w2.v3, 3) ASC

-- column user.city: Value [("user.city", "user.city")]