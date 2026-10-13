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
      'HAS_CREATOR' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.PersonId AS "__cg_start_0", 
      e.CommentId AS "__cg_end_0", 
      'Person' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('HAS_CREATOR:Comment:Person:', toString(tuple(e.CommentId, e.PersonId))) AS "__cg_key"
FROM ldbc.Comment_hasCreator_Person AS e

UNION ALL
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.PostId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.PostId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.PersonId AS "__cg_start_0", 
      e.PostId AS "__cg_end_0", 
      'Person' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('HAS_CREATOR:Post:Person:', toString(tuple(e.PostId, e.PersonId))) AS "__cg_key"
FROM ldbc.Post_hasCreator_Person AS e
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
)
SELECT 
      v6.id AS "personId", 
      v6.firstName AS "personFirstName", 
      v6.lastName AS "personLastName", 
      v4.creationDate AS "commentCreationDate", 
      v4.id AS "commentId", 
      v4.content AS "commentContent"
FROM ldbc.Person AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
JOIN v3_rels AS v3 ON v3.__cg_start_label = v2.__cg_label AND v3.__cg_start_0 = v2.__cg_id_0
JOIN ldbc.Comment AS v4 ON v3.__cg_end_0 = v4.id
JOIN ldbc.Comment_hasCreator_Person AS v5 ON v5.CommentId = v4.id
JOIN ldbc.Person AS v6 ON v5.PersonId = v6.id
WHERE (v0.id = 14 AND (v1.__cg_type <> 'HAS_CREATOR' OR v1.__cg_from_label <> 'Comment' OR v1.__cg_to_label <> 'Person' OR v1.__cg_rid_0 <> v5.CommentId OR v1.__cg_rid_1 <> v5.PersonId))
ORDER BY v4.creationDate DESC NULLS FIRST, v4.id ASC
LIMIT 20
-- column personId: Value [("personId", "personId")]
-- column personFirstName: Value [("personFirstName", "personFirstName")]
-- column personLastName: Value [("personLastName", "personLastName")]
-- column commentCreationDate: Value [("commentCreationDate", "commentCreationDate")]
-- column commentId: Value [("commentId", "commentId")]
-- column commentContent: Value [("commentContent", "commentContent")]