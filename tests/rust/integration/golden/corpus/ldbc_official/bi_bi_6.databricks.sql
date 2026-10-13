WITH v2_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`
FROM ldbc.Post AS e
), 
v1_rels AS (
SELECT 
      'HAS_TAG' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Tag' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.TagId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.TagId AS `__cg_to_0`, 
      e.TagId AS `__cg_start_0`, 
      e.CommentId AS `__cg_end_0`, 
      'Tag' AS `__cg_start_label`, 
      'Comment' AS `__cg_end_label`, 
      concat('HAS_TAG:Comment:Tag:', to_json(named_struct('_1', CAST(e.CommentId AS STRING), '_2', CAST(e.TagId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_hasTag_Tag AS e

UNION ALL
SELECT 
      'HAS_TAG' AS `__cg_type`, 
      'Post' AS `__cg_from_label`, 
      'Tag' AS `__cg_to_label`, 
      e.PostId AS `__cg_rid_0`, 
      e.TagId AS `__cg_rid_1`, 
      e.PostId AS `__cg_from_0`, 
      e.TagId AS `__cg_to_0`, 
      e.TagId AS `__cg_start_0`, 
      e.PostId AS `__cg_end_0`, 
      'Tag' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('HAS_TAG:Post:Tag:', to_json(named_struct('_1', CAST(e.PostId AS STRING), '_2', CAST(e.TagId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Post_hasTag_Tag AS e
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
v5_rels AS (
SELECT 
      'LIKES' AS `__cg_type`, 
      'Person' AS `__cg_from_label`, 
      'Comment' AS `__cg_to_label`, 
      e.PersonId AS `__cg_rid_0`, 
      e.CommentId AS `__cg_rid_1`, 
      e.PersonId AS `__cg_from_0`, 
      e.CommentId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.PersonId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Person' AS `__cg_end_label`, 
      concat('LIKES:Person:Comment:', to_json(named_struct('_1', CAST(e.PersonId AS STRING), '_2', CAST(e.CommentId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Person_likes_Comment AS e

UNION ALL
SELECT 
      'LIKES' AS `__cg_type`, 
      'Person' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.PersonId AS `__cg_rid_0`, 
      e.PostId AS `__cg_rid_1`, 
      e.PersonId AS `__cg_from_0`, 
      e.PostId AS `__cg_to_0`, 
      e.PostId AS `__cg_start_0`, 
      e.PersonId AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'Person' AS `__cg_end_label`, 
      concat('LIKES:Person:Post:', to_json(named_struct('_1', CAST(e.PersonId AS STRING), '_2', CAST(e.PostId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Person_likes_Post AS e
), 
optional_o4 AS (
SELECT 
      v2.__cg_label AS `v2____cg_label`, 
      v2.__cg_id_0 AS `v2____cg_id_0`, 
      v6.id AS `v6__id`
FROM v2_labels AS v2
JOIN v5_rels AS v5 ON v5.__cg_start_label = v2.__cg_label AND v5.__cg_start_0 = v2.__cg_id_0
JOIN ldbc.Person AS v6 ON v5.__cg_end_0 = v6.id
), 
with_w6 AS (
SELECT 
      v0.id AS `v0__id`, 
      v0.name AS `p2_v0_name`, 
      v2.__cg_label AS `v2____cg_label`, 
      v2.__cg_id_0 AS `v2____cg_id_0`, 
      v4.id AS `v4__id`, 
      v4.id AS `p2_v4_id`, 
      o4.v6__id AS `v6__id`
FROM ldbc.Tag AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
JOIN v3_rels AS v3 ON v3.__cg_start_label = v2.__cg_label AND v3.__cg_start_0 = v2.__cg_id_0
JOIN ldbc.Person AS v4 ON v3.__cg_end_0 = v4.id
LEFT JOIN optional_o4 AS o4 ON v2.__cg_label = o4.v2____cg_label AND v2.__cg_id_0 = o4.v2____cg_id_0
WHERE v0.name = 'Hamid_Karzai'
), 
optional_d7 AS (
SELECT DISTINCT 
      w6.v6__id AS `v6__id`
FROM with_w6 AS w6
), 
v8_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`
FROM ldbc.Post AS e
), 
v7_rels AS (
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
), 
v9_rels AS (
SELECT 
      'LIKES' AS `__cg_type`, 
      'Person' AS `__cg_from_label`, 
      'Comment' AS `__cg_to_label`, 
      e.PersonId AS `__cg_rid_0`, 
      e.CommentId AS `__cg_rid_1`, 
      e.PersonId AS `__cg_from_0`, 
      e.CommentId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.PersonId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Person' AS `__cg_end_label`, 
      concat('LIKES:Person:Comment:', to_json(named_struct('_1', CAST(e.PersonId AS STRING), '_2', CAST(e.CommentId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Person_likes_Comment AS e

UNION ALL
SELECT 
      'LIKES' AS `__cg_type`, 
      'Person' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.PersonId AS `__cg_rid_0`, 
      e.PostId AS `__cg_rid_1`, 
      e.PersonId AS `__cg_from_0`, 
      e.PostId AS `__cg_to_0`, 
      e.PostId AS `__cg_start_0`, 
      e.PersonId AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'Person' AS `__cg_end_label`, 
      concat('LIKES:Person:Post:', to_json(named_struct('_1', CAST(e.PersonId AS STRING), '_2', CAST(e.PostId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Person_likes_Post AS e
), 
optional_o8 AS (
SELECT 
      d7.v6__id AS `v6__id`, 
      v8.__cg_label AS `v8____cg_label`, 
      v8.__cg_id_0 AS `v8____cg_id_0`, 
      v9.__cg_type AS `v9____cg_type`, 
      v9.__cg_from_label AS `v9____cg_from_label`, 
      v9.__cg_to_label AS `v9____cg_to_label`, 
      v9.__cg_rid_0 AS `v9____cg_rid_0`, 
      v9.__cg_rid_1 AS `v9____cg_rid_1`, 
      v9.__cg_from_0 AS `v9____cg_from_0`, 
      v9.__cg_to_0 AS `v9____cg_to_0`, 
      v9.__cg_key AS `v9____cg_key`, 
      v10.id AS `v10__id`
FROM optional_d7 AS d7
JOIN v7_rels AS v7 ON v7.__cg_start_0 = d7.v6__id
JOIN v8_labels AS v8 ON v7.__cg_end_label = v8.__cg_label AND v7.__cg_end_0 = v8.__cg_id_0
JOIN v9_rels AS v9 ON v9.__cg_start_label = v8.__cg_label AND v9.__cg_start_0 = v8.__cg_id_0
JOIN ldbc.Person AS v10 ON v9.__cg_end_0 = v10.id
WHERE d7.v6__id IS NOT NULL
)
SELECT 
      w6.p2_v4_id AS `person1.id`, 
      count(DISTINCT CASE WHEN o8.v9____cg_type IS NULL THEN NULL ELSE struct(o8.v9____cg_type, o8.v9____cg_from_label, o8.v9____cg_to_label, o8.v9____cg_rid_0, o8.v9____cg_rid_1) END) AS `authorityScore`
FROM with_w6 AS w6
LEFT JOIN optional_o8 AS o8 ON w6.v6__id = o8.v6__id
GROUP BY w6.p2_v4_id
ORDER BY count(DISTINCT CASE WHEN o8.v9____cg_type IS NULL THEN NULL ELSE struct(o8.v9____cg_type, o8.v9____cg_from_label, o8.v9____cg_to_label, o8.v9____cg_rid_0, o8.v9____cg_rid_1) END) DESC NULLS FIRST, w6.p2_v4_id ASC NULLS LAST
LIMIT 100
-- column person1.id: Value [("person1.id", "person1.id")]
-- column authorityScore: Value [("authorityScore", "authorityScore")]