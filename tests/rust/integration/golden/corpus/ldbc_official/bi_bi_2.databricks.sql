WITH v3_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.creationDate AS `p2_v3_creationDate__cg0`, 
      NULL AS `p2_v3_creationDate__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v3_creationDate__cg0`, 
      e.creationDate AS `p2_v3_creationDate__cg1`
FROM ldbc.Post AS e
), 
v4_rels AS (
SELECT 
      'HAS_TAG' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Tag' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.TagId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.TagId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.TagId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Tag' AS `__cg_end_label`, 
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
      e.PostId AS `__cg_start_0`, 
      e.TagId AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'Tag' AS `__cg_end_label`, 
      concat('HAS_TAG:Post:Tag:', to_json(named_struct('_1', CAST(e.PostId AS STRING), '_2', CAST(e.TagId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Post_hasTag_Tag AS e
), 
optional_o1 AS (
SELECT 
      v0.id AS `v0__id`, 
      v3.__cg_label AS `v3____cg_label`, 
      v3.__cg_id_0 AS `v3____cg_id_0`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v3.p2_v3_creationDate__cg0, v3.p2_v3_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v3_creationDate`
FROM v3_labels AS v3
JOIN v4_rels AS v4 ON v4.__cg_start_label = v3.__cg_label AND v4.__cg_start_0 = v3.__cg_id_0
JOIN ldbc.Tag AS v0 ON v4.__cg_end_0 = v0.id
WHERE (1300000000000 <= CASE WHEN regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v3.p2_v3_creationDate__cg0, v3.p2_v3_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AND CASE WHEN regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v3.p2_v3_creationDate__cg0, v3.p2_v3_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END < unix_millis(timestamp_millis(1300000000000) + make_dt_interval(100, 0, 0, 0)))
), 
with_w4 AS (
SELECT 
      v0.id AS `v5__id`, 
      v0.name AS `p2_v5_name`, 
      count(o1.v3____cg_label) AS `v6`
FROM ldbc.Tag AS v0
JOIN ldbc.Tag_hasType_TagClass AS v1 ON v1.TagId = v0.id
JOIN ldbc.TagClass AS v2 ON v1.TagClassId = v2.id
LEFT JOIN optional_o1 AS o1 ON v0.id = o1.v0__id
WHERE v2.name = 'MilitaryPerson'
GROUP BY v0.id, v0.name
), 
v7_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.creationDate AS `p2_v7_creationDate__cg0`, 
      NULL AS `p2_v7_creationDate__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v7_creationDate__cg0`, 
      e.creationDate AS `p2_v7_creationDate__cg1`
FROM ldbc.Post AS e
), 
v8_rels AS (
SELECT 
      'HAS_TAG' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Tag' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.TagId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.TagId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.TagId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Tag' AS `__cg_end_label`, 
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
      e.PostId AS `__cg_start_0`, 
      e.TagId AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'Tag' AS `__cg_end_label`, 
      concat('HAS_TAG:Post:Tag:', to_json(named_struct('_1', CAST(e.PostId AS STRING), '_2', CAST(e.TagId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Post_hasTag_Tag AS e
), 
optional_o5 AS (
SELECT 
      v5.id AS `v5__id`, 
      v7.__cg_label AS `v7____cg_label`, 
      v7.__cg_id_0 AS `v7____cg_id_0`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v7_creationDate`
FROM v7_labels AS v7
JOIN v8_rels AS v8 ON v8.__cg_start_label = v7.__cg_label AND v8.__cg_start_0 = v7.__cg_id_0
JOIN ldbc.Tag AS v5 ON v8.__cg_end_0 = v5.id
WHERE (unix_millis(timestamp_millis(1300000000000) + make_dt_interval(100, 0, 0, 0)) <= CASE WHEN regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AND CASE WHEN regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END < unix_millis(timestamp_millis(1300000000000) + make_dt_interval(200, 0, 0, 0)))
), 
with_w8 AS (
SELECT 
      w4.v5__id AS `v9__id`, 
      w4.p2_v5_name AS `p2_v9_name`, 
      w4.v6 AS `v10`, 
      count(o5.v7____cg_label) AS `v11`
FROM with_w4 AS w4
LEFT JOIN optional_o5 AS o5 ON w4.v5__id = o5.v5__id
GROUP BY w4.v5__id, w4.p2_v5_name, w4.v6
)
SELECT 
      w8.p2_v9_name AS `tag.name`, 
      w8.v10 AS `countWindow1`, 
      w8.v11 AS `countWindow2`, 
      abs(w8.v10 - w8.v11) AS `diff`
FROM with_w8 AS w8
ORDER BY abs(w8.v10 - w8.v11) DESC NULLS FIRST, w8.p2_v9_name ASC NULLS LAST
LIMIT 100