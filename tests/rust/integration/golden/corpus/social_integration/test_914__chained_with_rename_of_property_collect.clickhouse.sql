WITH with_w1 AS (
SELECT 
      v0.age AS "v1"
FROM test_integration.users_test AS v0
), 
with_w2 AS (
SELECT 
      w1.v1 AS "v2"
FROM with_w1 AS w1
)
SELECT 
      coalesce(groupArray(w2.v2), []) AS "ages"
FROM with_w2 AS w2
