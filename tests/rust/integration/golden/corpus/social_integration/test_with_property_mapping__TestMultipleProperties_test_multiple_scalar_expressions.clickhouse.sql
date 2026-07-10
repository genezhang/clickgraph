WITH with_e_id_n_cte_0 AS (SELECT 
      u.full_name AS "n", 
      u.email_address AS "e", 
      u.user_id AS "id"
FROM test_integration.users_test AS u
)
SELECT 
      e_id_n.n AS "n", 
      e_id_n.e AS "e", 
      e_id_n.id AS "id"
FROM with_e_id_n_cte_0 AS e_id_n
LIMIT 1