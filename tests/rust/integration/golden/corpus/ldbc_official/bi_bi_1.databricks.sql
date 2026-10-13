WITH v0_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.creationDate AS `p2_v0_creationDate__cg0`, 
      NULL AS `p2_v0_creationDate__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v0_creationDate__cg0`, 
      e.creationDate AS `p2_v0_creationDate__cg1`
FROM ldbc.Post AS e
), 
with_w2 AS (
SELECT 
      count(v0.__cg_label) AS `v1`
FROM v0_labels AS v0
WHERE CASE WHEN regexp_replace(regexp_replace(typeof(v0.p2_v0_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v0.p2_v0_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v0.p2_v0_creationDate__cg0, v0.p2_v0_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END < 1354157106851
), 
with_w3 AS (
SELECT 
      double(w2.v1) AS `v2`
FROM with_w2 AS w2
), 
v3_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.content AS `p2_v3_content__cg0`, 
      NULL AS `p2_v3_content__cg1`, 
      e.creationDate AS `p2_v3_creationDate__cg0`, 
      NULL AS `p2_v3_creationDate__cg1`, 
      e.length AS `p2_v3_length__cg0`, 
      NULL AS `p2_v3_length__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v3_content__cg0`, 
      e.content AS `p2_v3_content__cg1`, 
      NULL AS `p2_v3_creationDate__cg0`, 
      e.creationDate AS `p2_v3_creationDate__cg1`, 
      NULL AS `p2_v3_length__cg0`, 
      e.length AS `p2_v3_length__cg1`
FROM ldbc.Post AS e
), 
with_w5 AS (
SELECT 
      w3.v2 AS `v4`, 
      v3.__cg_label AS `v5____cg_label`, 
      v3.__cg_id_0 AS `v5____cg_id_0`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v3.p2_v3_length__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v3.p2_v3_length__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v3.p2_v3_length__cg0, v3.p2_v3_length__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `p2_v5_length`, 
      year(timestamp_millis(CASE WHEN regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v3.p2_v3_creationDate__cg0, v3.p2_v3_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END)) AS `v6`
FROM with_w3 AS w3
CROSS JOIN v3_labels AS v3
WHERE (CASE WHEN regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v3.p2_v3_creationDate__cg0, v3.p2_v3_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END < 1354157106851 AND CASE WHEN regexp_replace(regexp_replace(typeof(v3.p2_v3_content__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v3.p2_v3_content__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v3.p2_v3_content__cg0, v3.p2_v3_content__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END IS NOT NULL)
), 
with_w6 AS (
SELECT 
      w5.v4 AS `v7`, 
      w5.v6 AS `v8`, 
      CAST(w5.v5____cg_label = 'Comment' AS BOOLEAN) AS `v9`, 
      CASE WHEN w5.p2_v5_length < 40 THEN 0 WHEN w5.p2_v5_length < 80 THEN 1 WHEN w5.p2_v5_length < 160 THEN 2 ELSE 3 END AS `v10`, 
      count(w5.v5____cg_label) AS `v11`, 
      coalesce(sum(w5.p2_v5_length), 0) / double(count(w5.v5____cg_label)) AS `v12`, 
      coalesce(sum(w5.p2_v5_length), 0) AS `v13`
FROM with_w5 AS w5
GROUP BY w5.v4, w5.v6, w5.v5____cg_label = 'Comment', CASE WHEN w5.p2_v5_length < 40 THEN 0 WHEN w5.p2_v5_length < 80 THEN 1 WHEN w5.p2_v5_length < 160 THEN 2 ELSE 3 END
)
SELECT 
      w6.v8 AS `year`, 
      w6.v9 AS `isComment`, 
      w6.v10 AS `lengthCategory`, 
      w6.v11 AS `messageCount`, 
      w6.v12 AS `averageMessageLength`, 
      w6.v13 AS `sumMessageLength`, 
      w6.v11 / w6.v7 AS `percentageOfMessages`
FROM with_w6 AS w6
ORDER BY w6.v8 DESC NULLS FIRST, w6.v9 ASC NULLS LAST, w6.v10 ASC NULLS LAST

-- column year: Value [("year", "year")]
-- column isComment: Value [("isComment", "isComment")]
-- column lengthCategory: Value [("lengthCategory", "lengthCategory")]
-- column messageCount: Value [("messageCount", "messageCount")]
-- column averageMessageLength: Value [("averageMessageLength", "averageMessageLength")]
-- column sumMessageLength: Value [("sumMessageLength", "sumMessageLength")]
-- column percentageOfMessages: Value [("percentageOfMessages", "percentageOfMessages")]