WITH v4_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.content AS `p2_v4_content__cg0`, 
      NULL AS `p2_v4_content__cg1`, 
      e.creationDate AS `p2_v4_creationDate__cg0`, 
      NULL AS `p2_v4_creationDate__cg1`, 
      e.id AS `p2_v4_id__cg0`, 
      NULL AS `p2_v4_id__cg1`, 
      NULL AS `p2_v4_imageFile`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v4_content__cg0`, 
      e.content AS `p2_v4_content__cg1`, 
      NULL AS `p2_v4_creationDate__cg0`, 
      e.creationDate AS `p2_v4_creationDate__cg1`, 
      NULL AS `p2_v4_id__cg0`, 
      e.id AS `p2_v4_id__cg1`, 
      e.imageFile AS `p2_v4_imageFile`
FROM ldbc.Post AS e
), 
v1_both AS (
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
v3_rels AS (
SELECT 
      'HAS_CREATOR' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Person' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.PersonId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.PersonId AS `__cg_to_0`, 
      e.PersonId AS `__cg_start_0`, 
      e.CommentId AS `__cg_end_0`, 
      'Person' AS `__cg_start_label`, 
      'Comment' AS `__cg_end_label`, 
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
      e.PersonId AS `__cg_start_0`, 
      e.PostId AS `__cg_end_0`, 
      'Person' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('HAS_CREATOR:Post:Person:', to_json(named_struct('_1', CAST(e.PostId AS STRING), '_2', CAST(e.PersonId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Post_hasCreator_Person AS e
)
SELECT 
      v2.id AS `personId`, 
      v2.firstName AS `personFirstName`, 
      v2.lastName AS `personLastName`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v4.p2_v4_id__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v4.p2_v4_id__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v4.p2_v4_id__cg0, v4.p2_v4_id__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `postOrCommentId`, 
      coalesce(CASE WHEN regexp_replace(regexp_replace(typeof(v4.p2_v4_content__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v4.p2_v4_content__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v4.p2_v4_content__cg0, v4.p2_v4_content__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, v4.p2_v4_imageFile) AS `postOrCommentContent`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v4.p2_v4_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v4.p2_v4_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `postOrCommentCreationDate`
FROM ldbc.Person AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.id
JOIN ldbc.Person AS v2 ON v1.__cg_end_0 = v2.id
JOIN v3_rels AS v3 ON v3.__cg_start_0 = v2.id
JOIN v4_labels AS v4 ON v3.__cg_end_label = v4.__cg_label AND v3.__cg_end_0 = v4.__cg_id_0
WHERE (v0.id = 14 AND CASE WHEN regexp_replace(regexp_replace(typeof(v4.p2_v4_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v4.p2_v4_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END < 1354157106851)
ORDER BY CASE WHEN regexp_replace(regexp_replace(typeof(v4.p2_v4_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v4.p2_v4_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END DESC NULLS FIRST, bigint(CASE WHEN regexp_replace(regexp_replace(typeof(v4.p2_v4_id__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v4.p2_v4_id__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v4.p2_v4_id__cg0, v4.p2_v4_id__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END) ASC NULLS LAST
LIMIT 20