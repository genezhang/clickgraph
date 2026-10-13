WITH v2_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Post AS e
), 
v1_rels AS (
SELECT 
      'HAS_TAG' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Tag' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.TagId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.TagId AS "__cg_to_0", 
      e.TagId AS "__cg_start_0", 
      e.CommentId AS "__cg_end_0", 
      'Tag' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('HAS_TAG:Comment:Tag:', toString(tuple(e.CommentId, e.TagId))) AS "__cg_key"
FROM ldbc.Comment_hasTag_Tag AS e

UNION ALL
SELECT 
      'HAS_TAG' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'Tag' AS "__cg_to_label", 
      e.PostId AS "__cg_rid_0", 
      e.TagId AS "__cg_rid_1", 
      e.PostId AS "__cg_from_0", 
      e.TagId AS "__cg_to_0", 
      e.TagId AS "__cg_start_0", 
      e.PostId AS "__cg_end_0", 
      'Tag' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('HAS_TAG:Post:Tag:', toString(tuple(e.PostId, e.TagId))) AS "__cg_key"
FROM ldbc.Post_hasTag_Tag AS e
), 
v3_rels AS (
SELECT 
      'REPLY_OF' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Comment' AS "__cg_to_label", 
      e.Comment1Id AS "__cg_rid_0", 
      e.Comment2Id AS "__cg_rid_1", 
      e.Comment1Id AS "__cg_from_0", 
      e.Comment2Id AS "__cg_to_0", 
      e.Comment2Id AS "__cg_start_0", 
      e.Comment1Id AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('REPLY_OF:Comment:Comment:', toString(tuple(e.Comment1Id, e.Comment2Id))) AS "__cg_key"
FROM ldbc.Comment_replyOf_Comment AS e

UNION ALL
SELECT 
      'REPLY_OF' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PostId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PostId AS "__cg_to_0", 
      e.PostId AS "__cg_start_0", 
      e.CommentId AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('REPLY_OF:Comment:Post:', toString(tuple(e.CommentId, e.PostId))) AS "__cg_key"
FROM ldbc.Comment_replyOf_Post AS e
), 
subquery_s4 AS (
SELECT 
      v7.CommentId AS "v4__id", 
      v0.id AS "v0__id", 
      CAST(count(*) > 0 AS Nullable(Bool)) AS "__value"
FROM ldbc.Comment_hasTag_Tag AS v7
JOIN ldbc.Tag AS v0 ON v7.TagId = v0.id
WHERE v0.name = 'Hamid_Karzai'
GROUP BY v7.CommentId, v0.id
)
SELECT 
      v6.name AS "relatedTag.name", 
      count(DISTINCT v4.id) AS "count"
FROM ldbc.Tag AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
JOIN v3_rels AS v3 ON v3.__cg_start_label = v2.__cg_label AND v3.__cg_start_0 = v2.__cg_id_0
JOIN ldbc.Comment AS v4 ON v3.__cg_end_0 = v4.id
JOIN ldbc.Comment_hasTag_Tag AS v5 ON v5.CommentId = v4.id
JOIN ldbc.Tag AS v6 ON v5.TagId = v6.id
LEFT JOIN subquery_s4 AS s4 ON v4.id = s4.v4__id AND v0.id = s4.v0__id
WHERE (v0.name = 'Hamid_Karzai' AND (v1.__cg_type <> 'HAS_TAG' OR v1.__cg_from_label <> 'Comment' OR v1.__cg_to_label <> 'Tag' OR v1.__cg_rid_0 <> v5.CommentId OR v1.__cg_rid_1 <> v5.TagId) AND NOT coalesce(s4.__value, false))
GROUP BY v6.name
ORDER BY count(DISTINCT v4.id) DESC NULLS FIRST, v6.name ASC
LIMIT 100
-- column relatedTag.name: Value [("relatedTag.name", "relatedTag.name")]
-- column count: Value [("count", "count")]