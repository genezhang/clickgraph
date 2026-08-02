WITH pattern_comp_u_0 AS (
SELECT node_id, groupArray(1) AS result FROM (SELECT follower_id AS node_id FROM social.user_follows_bench) GROUP BY node_id
)
SELECT 
      coalesce(__pc_0.result, []) AS "x"
FROM social.users_bench AS u
LEFT JOIN pattern_comp_u_0 AS __pc_0 ON u.user_id = __pc_0.node_id
