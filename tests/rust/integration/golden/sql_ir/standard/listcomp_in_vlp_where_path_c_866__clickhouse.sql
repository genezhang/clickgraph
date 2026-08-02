WITH RECURSIVE vlp_u_v AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [tuple(rel.follower_id, rel.followed_id)] as path_edges
    FROM social.users_bench AS start_node
    JOIN social.user_follows_bench AS rel ON start_node.user_id = rel.follower_id
    JOIN social.users_bench AS end_node ON rel.followed_id = end_node.user_id
    WHERE length(arrayFilter(x -> x > 1, [1, 2, 3])) > 0
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.follower_id, rel.followed_id)]) as path_edges
    FROM vlp_u_v vp
    JOIN social.user_follows_bench AS rel ON vp.end_id = rel.follower_id
    JOIN social.users_bench AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.follower_id, rel.followed_id))
      AND length(arrayFilter(x -> x > 1, [1, 2, 3])) > 0
)
SELECT 
      t.start_id AS "u.user_id"
FROM vlp_u_v AS t
