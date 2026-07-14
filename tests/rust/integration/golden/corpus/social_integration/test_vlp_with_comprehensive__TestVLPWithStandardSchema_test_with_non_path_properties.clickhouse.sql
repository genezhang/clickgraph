WITH RECURSIVE vlp_u1_u2 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        ['FOLLOWS'] as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges,
        start_node.full_name as start_name,
        end_node.full_name as end_name
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.user_id = 1
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_relationships, ['FOLLOWS']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges,
        vp.start_name as start_name,
        end_node.full_name as end_name
    FROM vlp_u1_u2 vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
with_end_name_start_name_cte_0 AS (SELECT 
      t.start_name AS "start_name", 
      t.end_name AS "end_name"
FROM vlp_u1_u2 AS t
WHERE start_name IS NOT NULL
)
SELECT 
      end_name_start_name.start_name AS "start_name", 
      end_name_start_name.end_name AS "end_name"
FROM with_end_name_start_name_cte_0 AS end_name_start_name
ORDER BY end_name_start_name.end_name ASC
LIMIT 5