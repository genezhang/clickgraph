WITH RECURSIVE vlp_a_b_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        array('TEST_FOLLOWS') as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.name = 'Alice'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        concat(vp.path_relationships, array('TEST_FOLLOWS')) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes
    FROM vlp_a_b_inner vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 5
      AND NOT array_contains(vp.path_nodes, end_node.user_id)
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE hop_count = (SELECT MIN(hop_count) FROM vlp_a_b_inner)
)
SELECT 
      count(*) AS `path_count`
FROM vlp_a_b AS t
