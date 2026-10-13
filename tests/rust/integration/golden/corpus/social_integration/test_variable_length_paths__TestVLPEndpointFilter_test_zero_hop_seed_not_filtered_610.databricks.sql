WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id) as path_nodes,
        (
            SELECT slice(array(__seed_edge.follow_id), 1, 0)
            FROM test_integration.user_follows_test AS __seed_edge
            LIMIT 1
        ) as path_edges
    FROM test_integration.users_test AS start_node
    WHERE (start_node.user_id > 2)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.followed_id
    JOIN test_integration.users_test AS end_node ON rel.follower_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
)
SELECT 
      v0.user_id AS `a.user_id`, 
      v2.user_id AS `b.user_id`
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.end_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.start_id = v2.user_id
WHERE v2.user_id > 2
ORDER BY v0.user_id ASC NULLS LAST, v2.user_id ASC NULLS LAST
