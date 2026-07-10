WITH with_combo_cte_0 AS (SELECT 
      concat(u.full_name, '@', u.email_address) AS "combo"
FROM test_integration.users_test AS u
)
SELECT 
      combo.combo AS "combo"
FROM with_combo_cte_0 AS combo
LIMIT 1