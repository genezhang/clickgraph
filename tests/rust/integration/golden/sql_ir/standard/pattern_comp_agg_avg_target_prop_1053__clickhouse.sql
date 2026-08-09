WITH pattern_comp_u_0 AS (
SELECT node_id, AVG(target_prop) AS result FROM (SELECT follower_id AS node_id, __tgt.user_id AS target_prop FROM social.user_follows_bench INNER JOIN social.users_bench AS __tgt ON followed_id = __tgt.user_id) GROUP BY node_id
)
SELECT 
      u.full_name AS "n", 
      coalesce(__pc_0.result, 0) AS "m"
FROM social.users_bench AS u
LEFT JOIN pattern_comp_u_0 AS __pc_0 ON u.user_id = __pc_0.node_id
