WITH with_status_cte_0 AS (SELECT 
      CASE WHEN u.is_active THEN u.full_name ELSE 'inactive' END AS `status`
FROM test_integration.users_test AS u
)
SELECT 
      status.status AS `status`
FROM with_status_cte_0 AS status
LIMIT 1