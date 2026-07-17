WITH RECURSIVE undir_edges_u1_u2_test_integration_user_follows_test AS (
    SELECT e.follower_id, e.followed_id, e.follow_date, e.follow_id, e.follower_id AS __cg_orig_from, e.followed_id AS __cg_orig_to FROM test_integration.user_follows_test AS e
    UNION ALL
    SELECT e.followed_id AS follower_id, e.follower_id AS followed_id, e.follow_date, e.follow_id, e.follower_id AS __cg_orig_from, e.followed_id AS __cg_orig_to FROM test_integration.user_follows_test AS e
),
vlp_u1_u2 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(rel.follow_id) as path_edges
    FROM test_integration.users_test AS start_node
    JOIN undir_edges_u1_u2_test_integration_user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.user_id = 1
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_u1_u2 vp
    JOIN undir_edges_u1_u2_test_integration_user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
), 
with_u2_cte_0 AS (SELECT 
      end_id AS `p2_u2_user_id`
FROM vlp_u1_u2 AS t
)
SELECT 
      u2.p2_u2_user_id AS `userId`, 
      count(DISTINCT p.post_id) AS `postCount`
FROM with_u2_cte_0 AS u2
GROUP BY u2.p2_u2_user_id
ORDER BY postCount DESC
LIMIT 5