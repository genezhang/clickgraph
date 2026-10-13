WITH with_w1 AS (
SELECT 
      upperUTF8(v0.email_address) AS "v1"
FROM test_integration.users_test AS v0
)
SELECT 
      w1.v1 AS "upperEmail"
FROM with_w1 AS w1
LIMIT 1
-- column upperEmail: Value [("upperEmail", "upperEmail")]