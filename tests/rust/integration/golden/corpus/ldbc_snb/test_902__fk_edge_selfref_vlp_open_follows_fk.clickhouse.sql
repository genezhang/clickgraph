WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.commentId as start_id,
        end_node.commentId as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.commentId, end_node.commentId] as path_nodes,
        [tuple(rel.commentId, rel.replyOfCommentId)] as path_edges
    FROM ldbc.comment AS start_node
    JOIN ldbc.comment AS rel ON start_node.commentId = rel.commentId
    JOIN ldbc.comment AS end_node ON rel.replyOfCommentId = end_node.commentId
    UNION ALL
    SELECT
        vp.start_id,
        end_node.commentId as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.commentId]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.commentId, rel.replyOfCommentId)]) as path_edges
    FROM vlp_v1_path vp
    JOIN ldbc.comment AS rel ON vp.end_id = rel.commentId
    JOIN ldbc.comment AS end_node ON rel.replyOfCommentId = end_node.commentId
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.commentId, rel.replyOfCommentId))
)
SELECT 
      count(*) AS "c"
FROM vlp_v1_path AS v1
JOIN ldbc.comment AS v0 ON v1.start_id = v0.commentId
JOIN ldbc.comment AS v2 ON v1.end_id = v2.commentId
