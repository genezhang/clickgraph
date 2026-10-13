WITH v0_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.id AS `p2_v0_id__cg0`, 
      NULL AS `p2_v0_id__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v0_id__cg0`, 
      e.id AS `p2_v0_id__cg1`
FROM ldbc.Post AS e
), 
v1_rels AS (
SELECT 
      'REPLY_OF' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Comment' AS `__cg_to_label`, 
      e.Comment1Id AS `__cg_rid_0`, 
      e.Comment2Id AS `__cg_rid_1`, 
      e.Comment1Id AS `__cg_from_0`, 
      e.Comment2Id AS `__cg_to_0`, 
      e.Comment2Id AS `__cg_start_0`, 
      e.Comment1Id AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Comment' AS `__cg_end_label`, 
      concat('REPLY_OF:Comment:Comment:', to_json(named_struct('_1', CAST(e.Comment1Id AS STRING), '_2', CAST(e.Comment2Id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_replyOf_Comment AS e

UNION ALL
SELECT 
      'REPLY_OF' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.PostId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.PostId AS `__cg_to_0`, 
      e.PostId AS `__cg_start_0`, 
      e.CommentId AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'Comment' AS `__cg_end_label`, 
      concat('REPLY_OF:Comment:Post:', to_json(named_struct('_1', CAST(e.CommentId AS STRING), '_2', CAST(e.PostId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_replyOf_Post AS e
), 
with_w3 AS (
SELECT 
      v0.__cg_label AS `v0____cg_label`, 
      v0.__cg_id_0 AS `v0____cg_id_0`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v0.p2_v0_id__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v0.p2_v0_id__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v0_id`, 
      v2.id AS `v2__id`, 
      v2.content AS `p2_v2_content`, 
      v2.creationDate AS `p2_v2_creationDate`, 
      v2.id AS `p2_v2_id`, 
      v4.id AS `v4__id`, 
      v4.firstName AS `p2_v4_firstName`, 
      v4.id AS `p2_v4_id`, 
      v4.lastName AS `p2_v4_lastName`
FROM v0_labels AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_label = v0.__cg_label AND v1.__cg_start_0 = v0.__cg_id_0
JOIN ldbc.Comment AS v2 ON v1.__cg_end_0 = v2.id
JOIN ldbc.Comment_hasCreator_Person AS v3 ON v3.CommentId = v2.id
JOIN ldbc.Person AS v4 ON v3.PersonId = v4.id
WHERE CASE WHEN regexp_replace(regexp_replace(typeof(v0.p2_v0_id__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v0.p2_v0_id__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END = 3
), 
optional_d4 AS (
SELECT DISTINCT 
      w3.v0____cg_id_0 AS `v0____cg_id_0`, 
      w3.v0____cg_label AS `v0____cg_label`, 
      w3.v4__id AS `v4__id`
FROM with_w3 AS w3
), 
v5_rels AS (
SELECT 
      'HAS_CREATOR' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Person' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.PersonId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.PersonId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.PersonId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Person' AS `__cg_end_label`, 
      concat('HAS_CREATOR:Comment:Person:', to_json(named_struct('_1', CAST(e.CommentId AS STRING), '_2', CAST(e.PersonId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_hasCreator_Person AS e

UNION ALL
SELECT 
      'HAS_CREATOR' AS `__cg_type`, 
      'Post' AS `__cg_from_label`, 
      'Person' AS `__cg_to_label`, 
      e.PostId AS `__cg_rid_0`, 
      e.PersonId AS `__cg_rid_1`, 
      e.PostId AS `__cg_from_0`, 
      e.PersonId AS `__cg_to_0`, 
      e.PostId AS `__cg_start_0`, 
      e.PersonId AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'Person' AS `__cg_end_label`, 
      concat('HAS_CREATOR:Post:Person:', to_json(named_struct('_1', CAST(e.PostId AS STRING), '_2', CAST(e.PersonId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Post_hasCreator_Person AS e
), 
v7_both AS (
SELECT 
      e.Person1Id AS `Person1Id`, 
      e.Person2Id AS `Person2Id`, 
      e.creationDate AS `creationDate`, 
      e.Person1Id AS `__cg_start_0`, 
      e.Person2Id AS `__cg_end_0`
FROM ldbc.Person_knows_Person AS e

UNION ALL
SELECT 
      e.Person1Id AS `Person1Id`, 
      e.Person2Id AS `Person2Id`, 
      e.creationDate AS `creationDate`, 
      e.Person2Id AS `__cg_start_0`, 
      e.Person1Id AS `__cg_end_0`
FROM ldbc.Person_knows_Person AS e
WHERE e.Person1Id <> e.Person2Id
), 
optional_o5 AS (
SELECT 
      d4.v0____cg_label AS `v0____cg_label`, 
      d4.v0____cg_id_0 AS `v0____cg_id_0`, 
      d4.v4__id AS `v4__id`, 
      v6.id AS `v6__id`, 
      v7.Person1Id AS `v7__Person1Id`, 
      v7.Person2Id AS `v7__Person2Id`
FROM optional_d4 AS d4
JOIN v5_rels AS v5 ON v5.__cg_start_label = d4.v0____cg_label AND v5.__cg_start_0 = d4.v0____cg_id_0
JOIN ldbc.Person AS v6 ON v5.__cg_end_0 = v6.id
JOIN v7_both AS v7 ON v7.__cg_start_0 = v6.id AND v7.__cg_end_0 = d4.v4__id
)
SELECT 
      w3.p2_v2_id AS `commentId`, 
      w3.p2_v2_content AS `commentContent`, 
      w3.p2_v2_creationDate AS `commentCreationDate`, 
      w3.p2_v4_id AS `replyAuthorId`, 
      w3.p2_v4_firstName AS `replyAuthorFirstName`, 
      w3.p2_v4_lastName AS `replyAuthorLastName`, 
      CASE WHEN o5.v7__Person1Id IS NULL THEN false ELSE true END AS `replyAuthorKnowsOriginalMessageAuthor`
FROM with_w3 AS w3
LEFT JOIN optional_o5 AS o5 ON w3.v0____cg_label = o5.v0____cg_label AND w3.v0____cg_id_0 = o5.v0____cg_id_0 AND w3.v4__id = o5.v4__id
ORDER BY w3.p2_v2_creationDate DESC NULLS FIRST, w3.p2_v4_id ASC NULLS LAST
