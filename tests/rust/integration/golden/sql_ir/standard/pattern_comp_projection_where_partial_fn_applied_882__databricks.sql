WITH pattern_comp_u_0 AS (
SELECT node_id, collect_list(target_prop) AS result FROM (SELECT follower_id AS node_id, __tgt.full_name AS target_prop FROM social.user_follows_bench INNER JOIN social.users_bench AS __tgt ON followed_id = __tgt.user_id WHERE (__tgt.age > 3 AND lower(__tgt.full_name) = 'x')) GROUP BY node_id
)
SELECT 
      coalesce(__pc_0.result, array()) AS `names`
FROM social.users_bench AS u
LEFT JOIN pattern_comp_u_0 AS __pc_0 ON u.user_id = __pc_0.node_id
