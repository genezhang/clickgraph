WITH RECURSIVE vlp_a_b_inner AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id) as path_nodes
    FROM test_integration.users_test AS start_node
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes
    FROM vlp_a_b_inner vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_nodes, end_node.user_id)
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE (end_id > 2)
)
SELECT 
      t.start_id AS `a.user_id`, 
      t.end_id AS `b.user_id`
FROM vlp_a_b AS t
ORDER BY t.start_id ASC, t.end_id ASC
