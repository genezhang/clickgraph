WITH RECURSIVE vlp_a_a AS (
    SELECT 
        start_node.commentId as start_id,
        end_node.commentId as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.commentId, end_node.commentId) as path_nodes,
        array(struct(start_node.commentId, end_node.commentId)) as path_edges
    FROM ldbc.comment start_node
    JOIN ldbc.comment end_node ON start_node.replyOfCommentId = end_node.commentId
    UNION ALL
    SELECT
        new_start.commentId as start_id,
        vp.end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(array(new_start.commentId), vp.path_nodes) as path_nodes,
        concat(array(struct(new_start.commentId, current_node.commentId)), vp.path_edges) as path_edges
    FROM vlp_a_a vp
    JOIN ldbc.comment current_node ON vp.start_id = current_node.commentId
    JOIN ldbc.comment new_start ON new_start.replyOfCommentId = current_node.commentId
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(new_start.commentId, current_node.commentId))
)
SELECT 
      count(*) AS `c`
FROM vlp_a_a AS t
WHERE t.start_id = t.end_id
