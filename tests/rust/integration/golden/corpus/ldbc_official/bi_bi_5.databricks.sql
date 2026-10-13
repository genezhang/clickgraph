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
      v5.__cg_type AS `v5____cg_type`, 
      v5.__cg_from_label AS `v5____cg_from_label`, 
      v5.__cg_to_label AS `v5____cg_to_label`, 
      v5.__cg_rid_0 AS `v5____cg_rid_0`, 
      v5.__cg_rid_1 AS `v5____cg_rid_1`, 
      v5.__cg_from_0 AS `v5____cg_from_0`, 
      v5.__cg_to_0 AS `v5____cg_to_0`, 
      v5.__cg_key AS `v5____cg_key`
FROM v2_labels AS v2
JOIN v5_rels AS v5 ON v5.__cg_start_label = v2.__cg_label AND v5.__cg_start_0 = v2.__cg_id_0
JOIN ldbc.Person AS v6 ON v5.__cg_end_0 = v6.id
), 
with_w6 AS (
SELECT 
      v4.id AS `v7__id`, 
      v4.id AS `p2_v7_id`, 
      v2.__cg_label AS `v8____cg_label`, 
      v2.__cg_id_0 AS `v8____cg_id_0`, 
      count(o4.v5____cg_type) AS `v9`
FROM ldbc.Tag AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
JOIN v3_rels AS v3 ON v3.__cg_start_label = v2.__cg_label AND v3.__cg_start_0 = v2.__cg_id_0
JOIN ldbc.Person AS v4 ON v3.__cg_end_0 = v4.id
LEFT JOIN optional_o4 AS o4 ON v2.__cg_label = o4.v2____cg_label AND v2.__cg_id_0 = o4.v2____cg_id_0
WHERE v0.name = 'Hamid_Karzai'
GROUP BY v4.id, v4.id, v2.__cg_label, v2.__cg_id_0
), 
v10_rels AS (
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
optional_o7 AS (
SELECT 
      v8.__cg_label AS `v8____cg_label`, 
      v8.__cg_id_0 AS `v8____cg_id_0`, 
      v11.id AS `v11__id`
FROM v2_labels AS v8
JOIN v10_rels AS v10 ON v10.__cg_start_label = v8.__cg_label AND v10.__cg_start_0 = v8.__cg_id_0
JOIN ldbc.Comment AS v11 ON v10.__cg_end_0 = v11.id
), 
with_w9 AS (
SELECT 
      w6.v7__id AS `v12__id`, 
      w6.p2_v7_id AS `p3_v12_id`, 
      w6.v8____cg_label AS `v13____cg_label`, 
      w6.v8____cg_id_0 AS `v13____cg_id_0`, 
      w6.v9 AS `v14`, 
      count(o7.v11__id) AS `v15`
FROM with_w6 AS w6
LEFT JOIN optional_o7 AS o7 ON w6.v8____cg_label = o7.v8____cg_label AND w6.v8____cg_id_0 = o7.v8____cg_id_0
GROUP BY w6.v7__id, w6.p2_v7_id, w6.v8____cg_label, w6.v8____cg_id_0, w6.v9
), 
with_w10 AS (
SELECT 
      w9.v12__id AS `v16__id`, 
      w9.p3_v12_id AS `p3_v16_id`, 
      count(w9.v13____cg_label) AS `v17`, 
      coalesce(sum(w9.v14), 0) AS `v18`, 
      coalesce(sum(w9.v15), 0) AS `v19`
FROM with_w9 AS w9
GROUP BY w9.v12__id, w9.p3_v12_id
)
SELECT 
      w10.p3_v16_id AS `person.id`, 
      w10.v19 AS `replyCount`, 
      w10.v18 AS `likeCount`, 
      w10.v17 AS `messageCount`, 
      1 * w10.v17 + 2 * w10.v19 + 10 * w10.v18 AS `score`
FROM with_w10 AS w10
ORDER BY 1 * w10.v17 + 2 * w10.v19 + 10 * w10.v18 DESC NULLS FIRST, w10.p3_v16_id ASC NULLS LAST
LIMIT 100