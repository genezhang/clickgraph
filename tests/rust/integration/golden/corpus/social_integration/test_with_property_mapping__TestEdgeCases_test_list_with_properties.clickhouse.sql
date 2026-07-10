WITH with_info_cte_0 AS (SELECT 
      [toString(u.full_name), toString(u.email_address)] AS "info"
FROM test_integration.users_test AS u
)
SELECT 
      info.info AS "info"
FROM with_info_cte_0 AS info
LIMIT 1