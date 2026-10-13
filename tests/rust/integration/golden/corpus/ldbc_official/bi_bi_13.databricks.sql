WITH with_w1 AS (
SELECT 
      v0.id AS `v5__id`, 
      v4.id AS `v6__id`, 
      v4.birthday AS `p2_v6_birthday`, 
      v4.browserUsed AS `p2_v6_browserUsed`, 
      v4.creationDate AS `p2_v6_creationDate`, 
      v4.email AS `p2_v6_email`, 
      v4.firstName AS `p2_v6_firstName`, 
      v4.gender AS `p2_v6_gender`, 
      v4.id AS `p2_v6_id`, 
      v4.lastName AS `p2_v6_lastName`, 
      v4.locationIP AS `p2_v6_locationIP`, 
      v4.speaks AS `p2_v6_speaks`
FROM ldbc.Place AS v0
JOIN ldbc.Place_isPartOf_Place AS v1 ON v1.Place2Id = v0.id
JOIN ldbc.Place AS v2 ON v1.Place1Id = v2.id
JOIN ldbc.Person_isLocatedIn_Place AS v3 ON v3.CityId = v2.id
JOIN ldbc.Person AS v4 ON v3.PersonId = v4.id
WHERE (((v0.type = 'Country')) AND ((v2.type = 'City')) AND v0.name = 'India' AND v4.creationDate < 1354157106851)
), 
v8_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.creationDate AS `p2_v8_creationDate__cg0`, 
      NULL AS `p2_v8_creationDate__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v8_creationDate__cg0`, 
      e.creationDate AS `p2_v8_creationDate__cg1`
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
optional_o2 AS (
SELECT 
      v6.id AS `v6__id`, 
      v8.__cg_label AS `v8____cg_label`, 
      v8.__cg_id_0 AS `v8____cg_id_0`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v8.p2_v8_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v8.p2_v8_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v8.p2_v8_creationDate__cg0, v8.p2_v8_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v8_creationDate`
FROM ldbc.Person AS v6
JOIN v7_rels AS v7 ON v7.__cg_start_0 = v6.id
JOIN v8_labels AS v8 ON v7.__cg_end_label = v8.__cg_label AND v7.__cg_end_0 = v8.__cg_id_0
WHERE CASE WHEN regexp_replace(regexp_replace(typeof(v8.p2_v8_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v8.p2_v8_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v8.p2_v8_creationDate__cg0, v8.p2_v8_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END < 1354157106851
), 
with_w5 AS (
SELECT 
      w1.v5__id AS `v9__id`, 
      w1.v6__id AS `v10__id`, 
      w1.p2_v6_birthday AS `p3_v10_birthday`, 
      w1.p2_v6_browserUsed AS `p3_v10_browserUsed`, 
      w1.p2_v6_creationDate AS `p3_v10_creationDate`, 
      w1.p2_v6_email AS `p3_v10_email`, 
      w1.p2_v6_firstName AS `p3_v10_firstName`, 
      w1.p2_v6_gender AS `p3_v10_gender`, 
      w1.p2_v6_id AS `p3_v10_id`, 
      w1.p2_v6_lastName AS `p3_v10_lastName`, 
      w1.p2_v6_locationIP AS `p3_v10_locationIP`, 
      w1.p2_v6_speaks AS `p3_v10_speaks`, 
      count(o2.v8____cg_label) AS `v11`
FROM with_w1 AS w1
LEFT JOIN optional_o2 AS o2 ON w1.v6__id = o2.v6__id
GROUP BY w1.v5__id, w1.v6__id, w1.p2_v6_birthday, w1.p2_v6_browserUsed, w1.p2_v6_creationDate, w1.p2_v6_email, w1.p2_v6_firstName, w1.p2_v6_gender, w1.p2_v6_id, w1.p2_v6_lastName, w1.p2_v6_locationIP, w1.p2_v6_speaks
), 
with_w6 AS (
SELECT 
      w5.v9__id AS `v12__id`, 
      w5.v10__id AS `v13__id`, 
      w5.p3_v10_birthday AS `p3_v13_birthday`, 
      w5.p3_v10_browserUsed AS `p3_v13_browserUsed`, 
      w5.p3_v10_creationDate AS `p3_v13_creationDate`, 
      w5.p3_v10_email AS `p3_v13_email`, 
      w5.p3_v10_firstName AS `p3_v13_firstName`, 
      w5.p3_v10_gender AS `p3_v13_gender`, 
      w5.p3_v10_id AS `p3_v13_id`, 
      w5.p3_v10_lastName AS `p3_v13_lastName`, 
      w5.p3_v10_locationIP AS `p3_v13_locationIP`, 
      w5.p3_v10_speaks AS `p3_v13_speaks`, 
      12 * (year(timestamp_millis(1354157106851)) - year(timestamp_millis(w5.p3_v10_creationDate))) + month(timestamp_millis(1354157106851)) - month(timestamp_millis(w5.p3_v10_creationDate)) + 1 AS `v14`, 
      w5.v11 AS `v15`
FROM with_w5 AS w5
WHERE w5.v11 / (12 * (year(timestamp_millis(1354157106851)) - year(timestamp_millis(w5.p3_v10_creationDate))) + month(timestamp_millis(1354157106851)) - month(timestamp_millis(w5.p3_v10_creationDate)) + 1) < 1
), 
with_w7 AS (
SELECT 
      w6.v12__id AS `v16__id`, 
      collect_list(CASE WHEN NOT isnull(w6.v13__id) THEN named_struct('_1', w6.v13__id, '_2', w6.p3_v13_birthday, '_3', w6.p3_v13_browserUsed, '_4', w6.p3_v13_creationDate, '_5', w6.p3_v13_email, '_6', w6.p3_v13_firstName, '_7', w6.p3_v13_gender, '_8', w6.p3_v13_id, '_9', w6.p3_v13_lastName, '_10', w6.p3_v13_locationIP, '_11', w6.p3_v13_speaks) END) AS `v17`
FROM with_w6 AS w6
GROUP BY w6.v12__id
), 
with_w8 AS (
SELECT 
      w7.v16__id AS `v16__id`, 
      w7.v17 AS `v17`, 
      __cg_element AS `v18`
FROM with_w7 AS w7
LATERAL VIEW explode(w7.v17) AS __cg_element
), 
optional_d9 AS (
SELECT DISTINCT 
      w8.v17 AS `v17`, 
      w8.v18 AS `v18`
FROM with_w8 AS w8
), 
v21_labels AS (
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
v20_rels AS (
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
v22_rels AS (
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
optional_o10 AS (
SELECT 
      d9.v17 AS `v17`, 
      d9.v18 AS `v18`, 
      v21.__cg_label AS `v21____cg_label`, 
      v21.__cg_id_0 AS `v21____cg_id_0`, 
      v23.id AS `v23__id`
FROM optional_d9 AS d9
CROSS JOIN ldbc.Person AS v19
JOIN v20_rels AS v20 ON v20.__cg_start_0 = v19.id
JOIN v21_labels AS v21 ON v20.__cg_end_label = v21.__cg_label AND v20.__cg_end_0 = v21.__cg_id_0
JOIN v22_rels AS v22 ON v22.__cg_start_label = v21.__cg_label AND v22.__cg_start_0 = v21.__cg_id_0
JOIN ldbc.Person AS v23 ON v22.__cg_end_0 = v23.id
WHERE (CAST(CASE WHEN d9.v17 IS NULL THEN NULL WHEN array_size(d9.v17) = 0 THEN false WHEN v23.id IS NULL THEN NULL ELSE array_size(filter(d9.v17, __cg_in -> (__cg_in)._1 = v23.id)) > 0 END AS BOOLEAN) AND v19.id = (d9.v18)._1)
), 
with_w14 AS (
SELECT 
      w8.v18 AS `v24`, 
      count(o10.v23__id) AS `v25`
FROM with_w8 AS w8
LEFT JOIN optional_o10 AS o10 ON (w8.v17 = o10.v17 OR (w8.v17 IS NULL AND o10.v17 IS NULL)) AND (w8.v18 = o10.v18 OR (w8.v18 IS NULL AND o10.v18 IS NULL))
GROUP BY w8.v18
), 
optional_d15 AS (
SELECT DISTINCT 
      w14.v24 AS `v24`
FROM with_w14 AS w14
), 
v28_labels AS (
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
v27_rels AS (
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
v29_rels AS (
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
optional_o16 AS (
SELECT 
      d15.v24 AS `v24`, 
      v28.__cg_label AS `v28____cg_label`, 
      v28.__cg_id_0 AS `v28____cg_id_0`, 
      v30.id AS `v30__id`, 
      v30.creationDate AS `p3_v30_creationDate`
FROM optional_d15 AS d15
CROSS JOIN ldbc.Person AS v26
JOIN v27_rels AS v27 ON v27.__cg_start_0 = v26.id
JOIN v28_labels AS v28 ON v27.__cg_end_label = v28.__cg_label AND v27.__cg_end_0 = v28.__cg_id_0
JOIN v29_rels AS v29 ON v29.__cg_start_label = v28.__cg_label AND v29.__cg_start_0 = v28.__cg_id_0
JOIN ldbc.Person AS v30 ON v29.__cg_end_0 = v30.id
WHERE (v30.creationDate < 1354157106851 AND v26.id = (d15.v24)._1)
), 
with_w20 AS (
SELECT 
      w14.v24 AS `v31`, 
      w14.v25 AS `v32`, 
      count(o16.v30__id) AS `v33`
FROM with_w14 AS w14
LEFT JOIN optional_o16 AS o16 ON w14.v24 = o16.v24 OR (w14.v24 IS NULL AND o16.v24 IS NULL)
GROUP BY w14.v24, w14.v25
)
SELECT 
      (w20.v31)._8 AS `zombie.id`, 
      w20.v32 AS `zombieLikeCount`, 
      w20.v33 AS `totalLikeCount`, 
      CASE w20.v33 WHEN 0 THEN 0 ELSE w20.v32 / double(w20.v33) END AS `zombieScore`
FROM with_w20 AS w20
ORDER BY CASE w20.v33 WHEN 0 THEN 0 ELSE w20.v32 / double(w20.v33) END DESC NULLS FIRST, (w20.v31)._8 ASC NULLS LAST
LIMIT 100