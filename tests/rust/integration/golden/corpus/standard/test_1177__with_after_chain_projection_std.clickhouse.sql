WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges,
        end_node.full_name as end_name
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges,
        end_node.full_name as end_name
    FROM vlp_a_b vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
with_n_cte_0 AS (SELECT 
      t.end_name AS "n"
FROM vlp_a_b AS t
JOIN test_integration.users_test AS c ON 1 = 1
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = c.user_id AND t0.followed_id = t.start_id
WHERE NOT has(t.path_edges, t0.follow_id)
)
SELECT 
      count(n.n) AS "count(n)"
FROM with_n_cte_0 AS n
