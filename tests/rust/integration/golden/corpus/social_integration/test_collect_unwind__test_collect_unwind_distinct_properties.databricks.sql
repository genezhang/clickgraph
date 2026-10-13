WITH with_w1 AS (
SELECT 
      v0.user_id AS `v1__user_id`, 
      collect_list(CASE WHEN NOT isnull(v0.user_id) THEN named_struct('_1', v0.user_id, '_2', v0.age, '_3', v0.city, '_4', v0.country, '_5', v0.email_address, '_6', v0.is_active, '_7', v0.full_name, '_8', v0.registration_date, '_9', v0.user_id) END) AS `v2`
FROM test_integration.users_test AS v0
GROUP BY v0.user_id
), 
with_w2 AS (
SELECT 
      w1.v1__user_id AS `v1__user_id`, 
      w1.v2 AS `v2`, 
      __cg_element AS `v3`
FROM with_w1 AS w1
LATERAL VIEW explode(w1.v2) AS __cg_element
)
SELECT DISTINCT 
      (w2.v3)._3 AS `user.city`
FROM with_w2 AS w2
ORDER BY `user.city` ASC NULLS LAST
