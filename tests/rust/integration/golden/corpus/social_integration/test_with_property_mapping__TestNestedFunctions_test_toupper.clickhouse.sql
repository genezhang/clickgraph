WITH with_upperEmail_cte_0 AS (SELECT 
      upper(u.email_address) AS "upperEmail"
FROM test_integration.users_test AS u
)
SELECT 
      upperEmail.upperEmail AS "upperEmail"
FROM with_upperEmail_cte_0 AS upperEmail
LIMIT 1