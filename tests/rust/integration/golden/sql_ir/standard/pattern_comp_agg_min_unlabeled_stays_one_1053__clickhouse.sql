WITH pattern_comp_u_0 AS (
SELECT node_id, MIN(1) AS result FROM (SELECT follower_id AS node_id FROM social.user_follows_bench) GROUP BY node_id
)
SELECT 
      u.full_name AS "n", 
      coalesce(__pc_0.result, 0) AS "m"
FROM social.users_bench AS u
LEFT JOIN pattern_comp_u_0 AS __pc_0 ON u.user_id = __pc_0.node_id
