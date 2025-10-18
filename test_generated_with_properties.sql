WITH RECURSIVE variable_path_69409a0a37164988b3b82729d988dc12 AS (
    SELECT
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        [start_node.user_id] as path_nodes,
        end_node.full_name as end_full_name,
        start_node.full_name as start_full_name
    FROM social.users start_node
    JOIN social.friendships rel ON start_node.user_id = rel.user1_id
    JOIN social.users end_node ON rel.user2_id = end_node.user_id
    WHERE start_node.user_id = 1
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_nodes, [current_node.user_id]) as path_nodes,
        end_node.full_name as end_full_name,
        vp.start_full_name as start_full_name
    FROM variable_path_69409a0a37164988b3b82729d988dc12 vp
    JOIN social.users current_node ON vp.end_id = current_node.user_id
    JOIN social.friendships rel ON current_node.user_id = rel.user1_id
    JOIN social.users end_node ON rel.user2_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_nodes, current_node.user_id)  -- Cycle detection
)
SELECT
      t.start_full_name as u1_full_name,
      t.end_full_name as u2_full_name
FROM variable_path_69409a0a37164988b3b82729d988dc12 AS t

SETTINGS max_recursive_cte_evaluation_depth = 1000
