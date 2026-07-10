WITH with_email_cte_0 AS (SELECT 
      u.email_address AS "email"
FROM test_integration.users_test AS u
)
SELECT 
      email.email AS "email"
FROM with_email_cte_0 AS email
LIMIT 1