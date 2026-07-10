WITH RECURSIVE vlp_u1_u2 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        array('TEST_FOLLOWS') as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        start_node.age as start_age,
        start_node.name as start_name,
        end_node.age as end_age,
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
        vp.start_age as start_age,
        vp.start_name as start_name,
        end_node.age as end_age,
        end_node.name as end_name
    FROM vlp_u1_u2 vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_nodes, end_node.user_id)
), 
with_path_len_u1_u2_cte_0 AS (SELECT 
      start_name AS `p2_u1_name`, 
      end_name AS `p2_u2_name`, 
      hop_count AS `path_len`
FROM vlp_u1_u2 AS t
WHERE hop_count = 2
)
SELECT 
      path_len_u1_u2.p2_u1_name AS `u1.name`, 
      path_len_u1_u2.p2_u2_name AS `u2.name`, 
      path_len_u1_u2.path_len AS `path_len`
FROM with_path_len_u1_u2_cte_0 AS path_len_u1_u2
