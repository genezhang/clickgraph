WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes
    FROM social.users_bench AS start_node
    JOIN social.user_follows_bench AS rel ON start_node.user_id = rel.follower_id
    JOIN social.users_bench AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes
    FROM vlp_a_b vp
    JOIN social.user_follows_bench AS rel ON vp.end_id = rel.follower_id
    JOIN social.users_bench AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_nodes, end_node.user_id)
)
SELECT 
      'FOLLOWS' AS "t"
FROM vlp_a_b AS t
