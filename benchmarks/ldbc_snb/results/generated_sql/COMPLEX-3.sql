-- LDBC Query: COMPLEX-3
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.808398
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (post:Post)<-[:REPLY_OF]-(c1:Comment)
-- OPTIONAL MATCH (c1)<-[:REPLY_OF*1..5]-(cn:Comment)
-- RETURN 
--     post.id AS postId,
--     count(DISTINCT c1) AS directReplies,
--     count(DISTINCT cn) AS deepReplies
-- ORDER BY deepReplies DESC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte6 AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.Comment1Id, rel.Comment2Id)] as path_edges,
        ['REPLY_OF::Comment::Comment'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Comment AS start_node
    JOIN ldbc.Comment_replyOf_Comment AS rel ON start_node.id = rel.Comment1Id
    JOIN ldbc.Comment AS end_node ON rel.Comment2Id = end_node.id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.Comment1Id, rel.Comment2Id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['REPLY_OF::Comment::Comment']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte6 vp
    JOIN ldbc.Comment AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Comment_replyOf_Comment AS rel ON current_node.id = rel.Comment1Id
    JOIN ldbc.Comment AS end_node ON rel.Comment2Id = end_node.id
    WHERE vp.hop_count < 5
      AND NOT has(vp.path_edges, tuple(rel.Comment1Id, rel.Comment2Id))
)
SELECT 
      post.id AS "postId", 
      count(DISTINCT c1.id) AS "directReplies", 
      count(DISTINCT cn.id) AS "deepReplies"
FROM ldbc.Comment AS c1
INNER JOIN ldbc.Comment_replyOf_Post AS t96 ON t96.CommentId = c1.id
INNER JOIN ldbc.Post AS post ON post.id = t96.PostId
LEFT JOIN vlp_cte6 AS vlp6 ON vlp6.start_id = cn.id
LEFT JOIN ldbc.Comment AS c1 ON vlp6.end_id = c1.id
GROUP BY post.id
ORDER BY deepReplies DESC
LIMIT  20
SETTINGS max_recursive_cte_evaluation_depth = 100

