WITH v2_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.browserUsed AS `p2_v2_browserUsed__cg0`, 
      NULL AS `p2_v2_browserUsed__cg1`, 
      e.content AS `p2_v2_content__cg0`, 
      NULL AS `p2_v2_content__cg1`, 
      e.creationDate AS `p2_v2_creationDate__cg0`, 
      NULL AS `p2_v2_creationDate__cg1`, 
      e.id AS `p2_v2_id__cg0`, 
      NULL AS `p2_v2_id__cg1`, 
      NULL AS `p2_v2_imageFile`, 
      NULL AS `p2_v2_language`, 
      e.length AS `p2_v2_length__cg0`, 
      NULL AS `p2_v2_length__cg1`, 
      e.locationIP AS `p2_v2_locationIP__cg0`, 
      NULL AS `p2_v2_locationIP__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v2_browserUsed__cg0`, 
      e.browserUsed AS `p2_v2_browserUsed__cg1`, 
      NULL AS `p2_v2_content__cg0`, 
      e.content AS `p2_v2_content__cg1`, 
      NULL AS `p2_v2_creationDate__cg0`, 
      e.creationDate AS `p2_v2_creationDate__cg1`, 
      NULL AS `p2_v2_id__cg0`, 
      e.id AS `p2_v2_id__cg1`, 
      e.imageFile AS `p2_v2_imageFile`, 
      e.language AS `p2_v2_language`, 
      NULL AS `p2_v2_length__cg0`, 
      e.length AS `p2_v2_length__cg1`, 
      NULL AS `p2_v2_locationIP__cg0`, 
      e.locationIP AS `p2_v2_locationIP__cg1`
FROM ldbc.Post AS e
), 
v1_rels AS (
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
v3_rels AS (
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
      concat('LIKES:Person:Comment:', to_json(named_struct('_1', CAST(e.PersonId AS STRING), '_2', CAST(e.CommentId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`, 
      e.creationDate AS `p2_v3_creationDate__cg0`, 
      NULL AS `p2_v3_creationDate__cg1`
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
      concat('LIKES:Person:Post:', to_json(named_struct('_1', CAST(e.PersonId AS STRING), '_2', CAST(e.PostId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`, 
      NULL AS `p2_v3_creationDate__cg0`, 
      e.creationDate AS `p2_v3_creationDate__cg1`
FROM ldbc.Person_likes_Post AS e
), 
with_w4 AS (
SELECT 
      v4.id AS `v5__id`, 
      v4.firstName AS `p2_v5_firstName`, 
      v4.id AS `p2_v5_id`, 
      v4.lastName AS `p2_v5_lastName`, 
      v2.__cg_label AS `v6____cg_label`, 
      v2.__cg_id_0 AS `v6____cg_id_0`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v2.p2_v2_browserUsed__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v2.p2_v2_browserUsed__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v2.p2_v2_browserUsed__cg0, v2.p2_v2_browserUsed__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v6_browserUsed`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v2.p2_v2_content__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v2.p2_v2_content__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v2.p2_v2_content__cg0, v2.p2_v2_content__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v6_content`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v2.p2_v2_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v2.p2_v2_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v6_creationDate`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v2.p2_v2_id__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v2.p2_v2_id__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v6_id`, 
      v2.p2_v2_imageFile AS `p2_v6_imageFile`, 
      v2.p2_v2_language AS `p2_v6_language`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v2.p2_v2_length__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v2.p2_v2_length__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v2.p2_v2_length__cg0, v2.p2_v2_length__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v6_length`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v2.p2_v2_locationIP__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v2.p2_v2_locationIP__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v2.p2_v2_locationIP__cg0, v2.p2_v2_locationIP__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v6_locationIP`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v3.p2_v3_creationDate__cg0, v3.p2_v3_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `v7`, 
      v0.id AS `v8__id`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v3.p2_v3_creationDate__cg0, v3.p2_v3_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `__o0`, 
      bigint(CASE WHEN regexp_replace(regexp_replace(typeof(v2.p2_v2_id__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v2.p2_v2_id__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END) AS `__o1`
FROM ldbc.Person AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
JOIN v3_rels AS v3 ON v3.__cg_start_label = v2.__cg_label AND v3.__cg_start_0 = v2.__cg_id_0
JOIN ldbc.Person AS v4 ON v3.__cg_end_0 = v4.id
WHERE v0.id = 14
), 
with_w5 AS (
SELECT 
      w4.v5__id AS `v5__id`, 
      w4.p2_v5_firstName AS `p2_v5_firstName`, 
      w4.p2_v5_id AS `p2_v5_id`, 
      w4.p2_v5_lastName AS `p2_v5_lastName`, 
      w4.v6____cg_label AS `v6____cg_label`, 
      w4.v6____cg_id_0 AS `v6____cg_id_0`, 
      w4.p2_v6_browserUsed AS `p2_v6_browserUsed`, 
      w4.p2_v6_content AS `p2_v6_content`, 
      w4.p2_v6_creationDate AS `p2_v6_creationDate`, 
      w4.p2_v6_id AS `p2_v6_id`, 
      w4.p2_v6_imageFile AS `p2_v6_imageFile`, 
      w4.p2_v6_language AS `p2_v6_language`, 
      w4.p2_v6_length AS `p2_v6_length`, 
      w4.p2_v6_locationIP AS `p2_v6_locationIP`, 
      w4.v7 AS `v7`, 
      w4.v8__id AS `v8__id`, 
      row_number() OVER (ORDER BY w4.__o0 DESC NULLS FIRST, w4.__o1 ASC NULLS LAST) AS `__cg_row`
FROM with_w4 AS w4
), 
with_w6 AS (
SELECT 
      w5.v5__id AS `v9__id`, 
      w5.p2_v5_firstName AS `p2_v9_firstName`, 
      w5.p2_v5_id AS `p2_v9_id`, 
      w5.p2_v5_lastName AS `p2_v9_lastName`, 
      CASE WHEN array_size(transform(array_sort(collect_list(CASE WHEN true THEN named_struct('k', w5.__cg_row, 'v', named_struct('_1', w5.p2_v6_content, '_2', w5.p2_v6_creationDate, '_3', w5.p2_v6_id, '_4', w5.p2_v6_imageFile, '_5', w5.v7)) END), (__cg_l, __cg_r) -> CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), __cg_t -> __cg_t.v)) > 0 THEN (try_element_at(transform(array_sort(collect_list(CASE WHEN true THEN named_struct('k', w5.__cg_row, 'v', named_struct('_1', w5.p2_v6_content, '_2', w5.p2_v6_creationDate, '_3', w5.p2_v6_id, '_4', w5.p2_v6_imageFile, '_5', w5.v7)) END), (__cg_l, __cg_r) -> CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), __cg_t -> __cg_t.v), 1))._1 ELSE NULL END AS `v10__k0`, 
      CASE WHEN array_size(transform(array_sort(collect_list(CASE WHEN true THEN named_struct('k', w5.__cg_row, 'v', named_struct('_1', w5.p2_v6_content, '_2', w5.p2_v6_creationDate, '_3', w5.p2_v6_id, '_4', w5.p2_v6_imageFile, '_5', w5.v7)) END), (__cg_l, __cg_r) -> CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), __cg_t -> __cg_t.v)) > 0 THEN (try_element_at(transform(array_sort(collect_list(CASE WHEN true THEN named_struct('k', w5.__cg_row, 'v', named_struct('_1', w5.p2_v6_content, '_2', w5.p2_v6_creationDate, '_3', w5.p2_v6_id, '_4', w5.p2_v6_imageFile, '_5', w5.v7)) END), (__cg_l, __cg_r) -> CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), __cg_t -> __cg_t.v), 1))._2 ELSE NULL END AS `v10__k1`, 
      CASE WHEN array_size(transform(array_sort(collect_list(CASE WHEN true THEN named_struct('k', w5.__cg_row, 'v', named_struct('_1', w5.p2_v6_content, '_2', w5.p2_v6_creationDate, '_3', w5.p2_v6_id, '_4', w5.p2_v6_imageFile, '_5', w5.v7)) END), (__cg_l, __cg_r) -> CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), __cg_t -> __cg_t.v)) > 0 THEN (try_element_at(transform(array_sort(collect_list(CASE WHEN true THEN named_struct('k', w5.__cg_row, 'v', named_struct('_1', w5.p2_v6_content, '_2', w5.p2_v6_creationDate, '_3', w5.p2_v6_id, '_4', w5.p2_v6_imageFile, '_5', w5.v7)) END), (__cg_l, __cg_r) -> CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), __cg_t -> __cg_t.v), 1))._3 ELSE NULL END AS `v10__k2`, 
      CASE WHEN array_size(transform(array_sort(collect_list(CASE WHEN true THEN named_struct('k', w5.__cg_row, 'v', named_struct('_1', w5.p2_v6_content, '_2', w5.p2_v6_creationDate, '_3', w5.p2_v6_id, '_4', w5.p2_v6_imageFile, '_5', w5.v7)) END), (__cg_l, __cg_r) -> CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), __cg_t -> __cg_t.v)) > 0 THEN (try_element_at(transform(array_sort(collect_list(CASE WHEN true THEN named_struct('k', w5.__cg_row, 'v', named_struct('_1', w5.p2_v6_content, '_2', w5.p2_v6_creationDate, '_3', w5.p2_v6_id, '_4', w5.p2_v6_imageFile, '_5', w5.v7)) END), (__cg_l, __cg_r) -> CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), __cg_t -> __cg_t.v), 1))._4 ELSE NULL END AS `v10__k3`, 
      CASE WHEN array_size(transform(array_sort(collect_list(CASE WHEN true THEN named_struct('k', w5.__cg_row, 'v', named_struct('_1', w5.p2_v6_content, '_2', w5.p2_v6_creationDate, '_3', w5.p2_v6_id, '_4', w5.p2_v6_imageFile, '_5', w5.v7)) END), (__cg_l, __cg_r) -> CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), __cg_t -> __cg_t.v)) > 0 THEN (try_element_at(transform(array_sort(collect_list(CASE WHEN true THEN named_struct('k', w5.__cg_row, 'v', named_struct('_1', w5.p2_v6_content, '_2', w5.p2_v6_creationDate, '_3', w5.p2_v6_id, '_4', w5.p2_v6_imageFile, '_5', w5.v7)) END), (__cg_l, __cg_r) -> CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), __cg_t -> __cg_t.v), 1))._5 ELSE NULL END AS `v10__k4`, 
      w5.v8__id AS `v11__id`
FROM with_w5 AS w5
GROUP BY w5.v5__id, w5.p2_v5_firstName, w5.p2_v5_id, w5.p2_v5_lastName, w5.v8__id
), 
v12_both AS (
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
subquery_s8 AS (
SELECT 
      v9.id AS `v9__id`, 
      v11.id AS `v11__id`, 
      CAST(count(*) > 0 AS BOOLEAN) AS `__value`
FROM ldbc.Person AS v9
JOIN v12_both AS v12 ON v12.__cg_start_0 = v9.id
JOIN ldbc.Person AS v11 ON v12.__cg_end_0 = v11.id
GROUP BY v9.id, v11.id
)
SELECT 
      w6.p2_v9_id AS `personId`, 
      w6.p2_v9_firstName AS `personFirstName`, 
      w6.p2_v9_lastName AS `personLastName`, 
      w6.v10__k4 AS `likeCreationDate`, 
      w6.v10__k2 AS `commentOrPostId`, 
      coalesce(w6.v10__k0, w6.v10__k3) AS `commentOrPostContent`, 
      bigint(floor(double(w6.v10__k4 - w6.v10__k1) / 1000) / 60) AS `minutesLatency`, 
      CAST(NOT coalesce(s8.__value, false) AS BOOLEAN) AS `isNew`
FROM with_w6 AS w6
LEFT JOIN subquery_s8 AS s8 ON w6.v9__id = s8.v9__id AND w6.v11__id = s8.v11__id
ORDER BY w6.v10__k4 DESC NULLS FIRST, bigint(w6.p2_v9_id) ASC NULLS LAST
LIMIT 20