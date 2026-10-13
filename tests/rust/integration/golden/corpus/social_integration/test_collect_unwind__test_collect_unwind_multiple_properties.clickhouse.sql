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
      row_number() OVER () AS "__cg_row"
FROM with_w1 AS w1
), 
with_w3 AS (
SELECT 
      w2.v1__user_id AS "v1__user_id", 
      w2.v2 AS "v2", 
      __cg_element AS "v3", 
      w2.__cg_row AS "__o0", 
      __cg_position AS "__o1"
FROM with_w2 AS w2
ARRAY JOIN w2.v2 AS __cg_element, arrayEnumerate(w2.v2) AS __cg_position
)
SELECT 
      tupleElement(w3.v3, 7) AS "user.name", 
      tupleElement(w3.v3, 5) AS "user.email", 
      tupleElement(w3.v3, 3) AS "user.city"
FROM with_w3 AS w3
ORDER BY w3.__o0 ASC, w3.__o1 ASC
LIMIT 3
-- column user.name: Value [("user.name", "user.name")]
-- column user.email: Value [("user.email", "user.email")]
-- column user.city: Value [("user.city", "user.city")]