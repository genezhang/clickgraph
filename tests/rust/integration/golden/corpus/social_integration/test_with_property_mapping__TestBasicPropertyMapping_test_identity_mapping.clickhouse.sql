WITH with_id_cte_0 AS (SELECT 
      u.user_id AS "id"
FROM test_integration.users_test AS u
)
SELECT 
      id.id AS "id"
FROM with_id_cte_0 AS id
LIMIT 1