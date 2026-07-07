WITH RECURSIVE vlp_u_f AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        array('FOLLOWS::User::User') as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        concat(vp.path_relationships, array('FOLLOWS::User::User')) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes
    FROM vlp_u_f vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_nodes, end_node.user_id)
)
SELECT 
      count(*) AS `count`
FROM vlp_u_f AS t
