WITH with_a_fc_cte_0 AS (SELECT 
      count(t0.followed_id) AS `fc`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
GROUP BY a.user_id
)
SELECT 
      min(a_fc.fc) AS `min(fc)`, 
      max(a_fc.fc) AS `max(fc)`, 
      sum(a_fc.fc) AS `sum(fc)`, 
      stddev_samp(a_fc.fc) AS `stDev(fc)`
FROM with_a_fc_cte_0 AS a_fc
