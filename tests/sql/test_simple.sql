SELECT
    t.start_id as user_id_1,
    t.end_id as user_id_2,
    t.hop_count,
    t.path_nodes
FROM (
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
    FROM (
        SELECT * FROM social.users start_node
        JOIN social.friendships rel ON start_node.user_id = rel.user1_id
        JOIN social.users end_node ON rel.user2_id = end_node.user_id
    ) vp
    JOIN social.users current_node ON vp.end_id = current_node.user_id
    JOIN social.friendships rel ON current_node.user_id = rel.user1_id
    JOIN social.users end_node ON rel.user2_id = end_node.user_id
    WHERE vp.hop_count < 2
) AS t
ORDER BY t.start_id, t.end_id
