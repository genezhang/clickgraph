WITH with_info_cte_0 AS (SELECT 
      array(string(u.full_name), string(u.email_address)) AS `info`
FROM test_integration.users_test AS u
)
SELECT 
      info.info AS `info`
FROM with_info_cte_0 AS info
LIMIT 1