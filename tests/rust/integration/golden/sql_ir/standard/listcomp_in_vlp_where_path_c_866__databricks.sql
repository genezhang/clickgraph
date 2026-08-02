WITH RECURSIVE vlp_u_v AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(struct(rel.follower_id, rel.followed_id)) as path_edges
    FROM social.users_bench AS start_node
    JOIN social.user_follows_bench AS rel ON start_node.user_id = rel.follower_id
    JOIN social.users_bench AS end_node ON rel.followed_id = end_node.user_id
    WHERE length(filter(array(1, 2, 3), x -> x > 1)) > 0
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.follower_id, rel.followed_id))) as path_edges
    FROM vlp_u_v vp
    JOIN social.user_follows_bench AS rel ON vp.end_id = rel.follower_id
    JOIN social.users_bench AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(rel.follower_id, rel.followed_id))
      AND length(filter(array(1, 2, 3), x -> x > 1)) > 0
)
SELECT 
      t.start_id AS `u.user_id`
FROM vlp_u_v AS t
