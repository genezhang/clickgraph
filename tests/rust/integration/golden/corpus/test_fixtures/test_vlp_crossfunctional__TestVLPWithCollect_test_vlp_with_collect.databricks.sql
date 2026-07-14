WITH RECURSIVE vlp_u1_u2 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        array('TEST_FOLLOWS') as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(struct(rel.follower_id, rel.followed_id)) as path_edges,
        start_node.name as start_name,
        end_node.name as end_name
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.user_id = 1
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        concat(vp.path_relationships, array('TEST_FOLLOWS')) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.follower_id, rel.followed_id))) as path_edges,
        vp.start_name as start_name,
        end_node.name as end_name
    FROM vlp_u1_u2 vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.follower_id, rel.followed_id))
)
SELECT 
      t.start_name AS `start`, 
      collect_list(t.end_name) AS `reached`
FROM vlp_u1_u2 AS t
GROUP BY t.start_name
