WITH optional_o1 AS (
SELECT 
      v0.id AS `v0__id`, 
      v1.PersonId AS `v1__PersonId`, 
      v1.TagId AS `v1__TagId`, 
      v2.id AS `v2__id`, 
      v2.birthday AS `p2_v2_birthday`, 
      v2.browserUsed AS `p2_v2_browserUsed`, 
      v2.creationDate AS `p2_v2_creationDate`, 
      v2.email AS `p2_v2_email`, 
      v2.firstName AS `p2_v2_firstName`, 
      v2.gender AS `p2_v2_gender`, 
      v2.id AS `p2_v2_id`, 
      v2.lastName AS `p2_v2_lastName`, 
      v2.locationIP AS `p2_v2_locationIP`, 
      v2.speaks AS `p2_v2_speaks`
FROM ldbc.Tag AS v0
JOIN ldbc.Person_hasInterest_Tag AS v1 ON v1.TagId = v0.id
JOIN ldbc.Person AS v2 ON v1.PersonId = v2.id
WHERE v0.name = 'Hamid_Karzai'
), 
with_w2 AS (
SELECT 
      v0.id AS `v3__id`, 
      array_sort(collect_list(CASE WHEN NOT isnull(o1.v2__id) THEN named_struct('_1', o1.v2__id, '_2', o1.p2_v2_birthday, '_3', o1.p2_v2_browserUsed, '_4', o1.p2_v2_creationDate, '_5', o1.p2_v2_email, '_6', o1.p2_v2_firstName, '_7', o1.p2_v2_gender, '_8', o1.p2_v2_id, '_9', o1.p2_v2_lastName, '_10', o1.p2_v2_locationIP, '_11', o1.p2_v2_speaks) END), (__cg_sl, __cg_sr) -> CASE WHEN concat(typeof(__cg_sl), ':', to_json(named_struct('v', __cg_sl), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSSXXX', 'timestampNTZFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSS'))) < concat(typeof(__cg_sr), ':', to_json(named_struct('v', __cg_sr), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSSXXX', 'timestampNTZFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSS'))) THEN -1 WHEN concat(typeof(__cg_sl), ':', to_json(named_struct('v', __cg_sl), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSSXXX', 'timestampNTZFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSS'))) > concat(typeof(__cg_sr), ':', to_json(named_struct('v', __cg_sr), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSSXXX', 'timestampNTZFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSS'))) THEN 1 ELSE 0 END) AS `v4`
FROM ldbc.Tag AS v0
LEFT JOIN optional_o1 AS o1 ON v0.id = o1.v0__id
WHERE v0.name = 'Hamid_Karzai'
GROUP BY v0.id
), 
optional_d3 AS (
SELECT DISTINCT 
      w2.v3__id AS `v3__id`
FROM with_w2 AS w2
), 
v6_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.creationDate AS `p2_v6_creationDate__cg0`, 
      NULL AS `p2_v6_creationDate__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v6_creationDate__cg0`, 
      e.creationDate AS `p2_v6_creationDate__cg1`
FROM ldbc.Post AS e
), 
v5_rels AS (
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
v7_rels AS (
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
optional_o4 AS (
SELECT 
      d3.v3__id AS `v3__id`, 
      v6.__cg_label AS `v6____cg_label`, 
      v6.__cg_id_0 AS `v6____cg_id_0`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v6.p2_v6_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v6.p2_v6_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v6.p2_v6_creationDate__cg0, v6.p2_v6_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v6_creationDate`, 
      v8.id AS `v8__id`, 
      v8.birthday AS `p2_v8_birthday`, 
      v8.browserUsed AS `p2_v8_browserUsed`, 
      v8.creationDate AS `p2_v8_creationDate`, 
      v8.email AS `p2_v8_email`, 
      v8.firstName AS `p2_v8_firstName`, 
      v8.gender AS `p2_v8_gender`, 
      v8.id AS `p2_v8_id`, 
      v8.lastName AS `p2_v8_lastName`, 
      v8.locationIP AS `p2_v8_locationIP`, 
      v8.speaks AS `p2_v8_speaks`
FROM optional_d3 AS d3
JOIN v5_rels AS v5 ON v5.__cg_start_0 = d3.v3__id
JOIN v6_labels AS v6 ON v5.__cg_end_label = v6.__cg_label AND v5.__cg_end_0 = v6.__cg_id_0
JOIN v7_rels AS v7 ON v7.__cg_start_label = v6.__cg_label AND v7.__cg_start_0 = v6.__cg_id_0
JOIN ldbc.Person AS v8 ON v7.__cg_end_0 = v8.id
WHERE (1262305305684 < CASE WHEN regexp_replace(regexp_replace(typeof(v6.p2_v6_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v6.p2_v6_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v6.p2_v6_creationDate__cg0, v6.p2_v6_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AND CASE WHEN regexp_replace(regexp_replace(typeof(v6.p2_v6_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v6.p2_v6_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v6.p2_v6_creationDate__cg0, v6.p2_v6_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END < 1354157106851)
), 
with_w8 AS (
SELECT 
      w2.v3__id AS `v9__id`, 
      w2.v4 AS `v10`, 
      concat(w2.v4, array_sort(collect_list(CASE WHEN NOT isnull(o4.v8__id) THEN named_struct('_1', o4.v8__id, '_2', o4.p2_v8_birthday, '_3', o4.p2_v8_browserUsed, '_4', o4.p2_v8_creationDate, '_5', o4.p2_v8_email, '_6', o4.p2_v8_firstName, '_7', o4.p2_v8_gender, '_8', o4.p2_v8_id, '_9', o4.p2_v8_lastName, '_10', o4.p2_v8_locationIP, '_11', o4.p2_v8_speaks) END), (__cg_sl, __cg_sr) -> CASE WHEN concat(typeof(__cg_sl), ':', to_json(named_struct('v', __cg_sl), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSSXXX', 'timestampNTZFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSS'))) < concat(typeof(__cg_sr), ':', to_json(named_struct('v', __cg_sr), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSSXXX', 'timestampNTZFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSS'))) THEN -1 WHEN concat(typeof(__cg_sl), ':', to_json(named_struct('v', __cg_sl), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSSXXX', 'timestampNTZFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSS'))) > concat(typeof(__cg_sr), ':', to_json(named_struct('v', __cg_sr), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSSXXX', 'timestampNTZFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSS'))) THEN 1 ELSE 0 END)) AS `v11`
FROM with_w2 AS w2
LEFT JOIN optional_o4 AS o4 ON w2.v3__id = o4.v3__id
GROUP BY w2.v3__id, w2.v4
), 
with_w9 AS (
SELECT 
      w8.v9__id AS `v9__id`, 
      w8.v10 AS `v10`, 
      w8.v11 AS `v11`, 
      __cg_element AS `v12`
FROM with_w8 AS w8
LATERAL VIEW explode(w8.v11) AS __cg_element
), 
with_w10 AS (
SELECT DISTINCT 
      w9.v9__id AS `v13__id`, 
      w9.v12 AS `v14`
FROM with_w9 AS w9
), 
subquery_d11 AS (
SELECT DISTINCT 
      w10.v13__id AS `v13__id`, 
      w10.v14 AS `v14`
FROM with_w10 AS w10
), 
subquery_s12 AS (
SELECT 
      d11.v13__id AS `v13__id`, 
      d11.v14 AS `v14`, 
      count(*) AS `__value`
FROM subquery_d11 AS d11
JOIN ldbc.Person_hasInterest_Tag AS v15 ON v15.TagId = d11.v13__id
JOIN ldbc.Person AS v16 ON v15.PersonId = v16.id
WHERE v16.id = (d11.v14)._1
GROUP BY d11.v13__id, d11.v14
), 
subquery_d13 AS (
SELECT DISTINCT 
      w10.v13__id AS `v13__id`, 
      w10.v14 AS `v14`
FROM with_w10 AS w10
), 
v19_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.creationDate AS `p3_v19_creationDate__cg0`, 
      NULL AS `p3_v19_creationDate__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p3_v19_creationDate__cg0`, 
      e.creationDate AS `p3_v19_creationDate__cg1`
FROM ldbc.Post AS e
), 
v18_rels AS (
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
v20_rels AS (
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
subquery_s17 AS (
SELECT 
      d13.v13__id AS `v13__id`, 
      d13.v14 AS `v14`, 
      count(*) AS `__value`
FROM subquery_d13 AS d13
JOIN v18_rels AS v18 ON v18.__cg_start_0 = d13.v13__id
JOIN v19_labels AS v19 ON v18.__cg_end_label = v19.__cg_label AND v18.__cg_end_0 = v19.__cg_id_0
JOIN v20_rels AS v20 ON v20.__cg_start_label = v19.__cg_label AND v20.__cg_start_0 = v19.__cg_id_0
JOIN ldbc.Person AS v21 ON v20.__cg_end_0 = v21.id
WHERE ((1262305305684 < CASE WHEN regexp_replace(regexp_replace(typeof(v19.p3_v19_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v19.p3_v19_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v19.p3_v19_creationDate__cg0, v19.p3_v19_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AND CASE WHEN regexp_replace(regexp_replace(typeof(v19.p3_v19_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v19.p3_v19_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v19.p3_v19_creationDate__cg0, v19.p3_v19_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END < 1354157106851) AND v21.id = (d13.v14)._1)
GROUP BY d13.v13__id, d13.v14
), 
with_w18 AS (
SELECT 
      w10.v13__id AS `v23__id`, 
      w10.v14 AS `v24`, 
      100 * coalesce(s12.__value, 0) + coalesce(s17.__value, 0) AS `v25`
FROM with_w10 AS w10
LEFT JOIN subquery_s12 AS s12 ON w10.v13__id = s12.v13__id AND ((w10.v14 <=> s12.v14) AND true)
LEFT JOIN subquery_s17 AS s17 ON w10.v13__id = s17.v13__id AND ((w10.v14 <=> s17.v14) AND true)
), 
optional_d19 AS (
SELECT DISTINCT 
      w18.v24 AS `v24`
FROM with_w18 AS w18
), 
v27_both AS (
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
optional_o20 AS (
SELECT 
      d19.v24 AS `v24`, 
      v28.id AS `v28__id`
FROM optional_d19 AS d19
CROSS JOIN ldbc.Person AS v26
JOIN v27_both AS v27 ON v27.__cg_start_0 = v26.id
JOIN ldbc.Person AS v28 ON v27.__cg_end_0 = v28.id
WHERE v26.id = (d19.v24)._1
), 
subquery_s22 AS (
SELECT 
      v29.TagId AS `v23__id`, 
      v29.PersonId AS `v28__id`, 
      count(*) AS `__value`
FROM ldbc.Person_hasInterest_Tag AS v29
WHERE v29.PersonId IS NOT NULL
GROUP BY v29.TagId, v29.PersonId
), 
subquery_d23 AS (
SELECT DISTINCT 
      w18.v23__id AS `v23__id`, 
      o20.v28__id AS `v28__id`
FROM with_w18 AS w18
LEFT JOIN optional_o20 AS o20 ON w18.v24 = o20.v24 OR (w18.v24 IS NULL AND o20.v24 IS NULL)
), 
v32_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.creationDate AS `p3_v32_creationDate__cg0`, 
      NULL AS `p3_v32_creationDate__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p3_v32_creationDate__cg0`, 
      e.creationDate AS `p3_v32_creationDate__cg1`
FROM ldbc.Post AS e
), 
v31_rels AS (
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
v33_rels AS (
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
subquery_s27 AS (
SELECT 
      d23.v23__id AS `v23__id`, 
      d23.v28__id AS `v28__id`, 
      count(*) AS `__value`
FROM subquery_d23 AS d23
JOIN v31_rels AS v31 ON v31.__cg_start_0 = d23.v23__id
JOIN v32_labels AS v32 ON v31.__cg_end_label = v32.__cg_label AND v31.__cg_end_0 = v32.__cg_id_0
JOIN v33_rels AS v33 ON v33.__cg_start_label = v32.__cg_label AND v33.__cg_start_0 = v32.__cg_id_0 AND v33.__cg_end_0 = d23.v28__id
WHERE (d23.v28__id IS NOT NULL AND (1262305305684 < CASE WHEN regexp_replace(regexp_replace(typeof(v32.p3_v32_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v32.p3_v32_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v32.p3_v32_creationDate__cg0, v32.p3_v32_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AND CASE WHEN regexp_replace(regexp_replace(typeof(v32.p3_v32_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v32.p3_v32_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v32.p3_v32_creationDate__cg0, v32.p3_v32_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END < 1354157106851))
GROUP BY d23.v23__id, d23.v28__id
), 
with_w28 AS (
SELECT 
      w18.v24 AS `v35`, 
      w18.v25 AS `v36`, 
      100 * coalesce(s22.__value, 0) + coalesce(s27.__value, 0) AS `v37`
FROM with_w18 AS w18
LEFT JOIN optional_o20 AS o20 ON w18.v24 = o20.v24 OR (w18.v24 IS NULL AND o20.v24 IS NULL)
LEFT JOIN subquery_s22 AS s22 ON w18.v23__id = s22.v23__id AND o20.v28__id = s22.v28__id
LEFT JOIN subquery_s27 AS s27 ON w18.v23__id = s27.v23__id AND o20.v28__id = s27.v28__id
)
SELECT 
      (w28.v35)._8 AS `person.id`, 
      w28.v36 AS `score`, 
      coalesce(sum(w28.v37), 0) AS `friendsScore`
FROM with_w28 AS w28
GROUP BY (w28.v35)._8, w28.v36
ORDER BY w28.v36 + coalesce(sum(w28.v37), 0) DESC NULLS FIRST, (w28.v35)._8 ASC NULLS LAST
LIMIT 100
-- column person.id: Value [("person.id", "person.id")]
-- column score: Value [("score", "score")]
-- column friendsScore: Value [("friendsScore", "friendsScore")]