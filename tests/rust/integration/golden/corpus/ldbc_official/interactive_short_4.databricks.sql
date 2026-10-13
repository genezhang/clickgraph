WITH v0_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.content AS `p2_v0_content__cg0`, 
      NULL AS `p2_v0_content__cg1`, 
      e.creationDate AS `p2_v0_creationDate__cg0`, 
      NULL AS `p2_v0_creationDate__cg1`, 
      e.id AS `p2_v0_id__cg0`, 
      NULL AS `p2_v0_id__cg1`, 
      NULL AS `p2_v0_imageFile`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v0_content__cg0`, 
      e.content AS `p2_v0_content__cg1`, 
      NULL AS `p2_v0_creationDate__cg0`, 
      e.creationDate AS `p2_v0_creationDate__cg1`, 
      NULL AS `p2_v0_id__cg0`, 
      e.id AS `p2_v0_id__cg1`, 
      e.imageFile AS `p2_v0_imageFile`
FROM ldbc.Post AS e
)
SELECT 
      CASE WHEN regexp_replace(regexp_replace(typeof(v0.p2_v0_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v0.p2_v0_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v0.p2_v0_creationDate__cg0, v0.p2_v0_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `messageCreationDate`, 
      coalesce(CASE WHEN regexp_replace(regexp_replace(typeof(v0.p2_v0_content__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v0.p2_v0_content__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v0.p2_v0_content__cg0, v0.p2_v0_content__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, v0.p2_v0_imageFile) AS `messageContent`
FROM v0_labels AS v0
WHERE CASE WHEN regexp_replace(regexp_replace(typeof(v0.p2_v0_id__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v0.p2_v0_id__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END = 3
