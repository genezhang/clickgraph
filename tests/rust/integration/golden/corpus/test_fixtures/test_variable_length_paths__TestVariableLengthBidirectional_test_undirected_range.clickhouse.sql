WITH RECURSIVE undir_edges_a_b_test_integration_follows AS (
    SELECT e.follower_id, e.followed_id, e.since, e.follower_id AS __cg_orig_from, e.followed_id AS __cg_orig_to FROM test_integration.follows AS e
    UNION ALL
    SELECT e.followed_id AS follower_id, e.follower_id AS followed_id, e.since, e.follower_id AS __cg_orig_from, e.followed_id AS __cg_orig_to FROM test_integration.follows AS e
),
vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [tuple(rel.__cg_orig_from, rel.__cg_orig_to)] as path_edges,
        end_node.name as end_name
    FROM test_integration.users AS start_node
    JOIN undir_edges_a_b_test_integration_follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.name = 'Bob'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.__cg_orig_from, rel.__cg_orig_to)]) as path_edges,
        end_node.name as end_name
    FROM vlp_a_b vp
    JOIN undir_edges_a_b_test_integration_follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.__cg_orig_from, rel.__cg_orig_to))
)
SELECT DISTINCT 
      t.end_name AS "b.name"
FROM vlp_a_b AS t
ORDER BY t.end_name ASC
