WITH with_cnt_prop_cte_0 AS (SELECT 
      a.user_id AS "prop", 
      count(r.follow_id) AS "cnt"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS r ON r.follower_id = a.user_id
GROUP BY a.user_id
)
SELECT 
      cnt_prop.prop AS "prop", 
      cnt_prop.cnt AS "cnt"
FROM with_cnt_prop_cte_0 AS cnt_prop
ORDER BY cnt_prop.cnt DESC
LIMIT 10