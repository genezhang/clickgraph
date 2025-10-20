WITH variable_path_6d9ab57a1e714813803ec622bde98e26 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        [start_node.user_id] as path_nodes
    FROM social.users start_node
    JOIN social.friendships rel ON start_node.user_id = rel.user1_id
    JOIN social.users end_node ON rel.user2_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_nodes, [current_node.user_id]) as path_nodes
    FROM variable_path_6d9ab57a1e714813803ec622bde98e26 vp
    JOIN social.users current_node ON vp.end_id = current_node.user_id
    JOIN social.friendships rel ON current_node.user_id = rel.user1_id
    JOIN social.users end_node ON rel.user2_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_nodes, current_node.user_id)  -- Cycle detection
)
SELECT 
      u1.user_id, 
      u2.user_id
FROM variable_path_6d9ab57a1e714813803ec622bde98e26 AS t

