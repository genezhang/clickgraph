WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.commentId as start_id,
        end_node.commentId as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.commentId, end_node.commentId) as path_nodes,
        array(struct(rel.commentId, rel.replyOfCommentId)) as path_edges
    FROM ldbc.comment AS start_node
    JOIN ldbc.comment AS rel ON start_node.commentId = rel.commentId
    JOIN ldbc.comment AS end_node ON rel.replyOfCommentId = end_node.commentId
    UNION ALL
    SELECT
        vp.start_id,
        end_node.commentId as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.commentId)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.commentId, rel.replyOfCommentId))) as path_edges
    FROM vlp_v1_path vp
    JOIN ldbc.comment AS rel ON vp.end_id = rel.commentId
    JOIN ldbc.comment AS end_node ON rel.replyOfCommentId = end_node.commentId
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.commentId, rel.replyOfCommentId))
)
SELECT 
      count(*) AS `c`
FROM vlp_v1_path AS v1
JOIN ldbc.comment AS v0 ON v1.start_id = v0.commentId AND v1.end_id = v0.commentId
