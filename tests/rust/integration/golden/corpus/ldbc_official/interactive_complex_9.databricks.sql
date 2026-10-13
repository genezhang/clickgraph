WITH RECURSIVE v1_both AS (
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
vlp_v1_path AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.id, end_node.id) as path_nodes,
        array(struct(rel.Person1Id, rel.Person2Id)) as path_edges
    FROM ldbc.Person AS start_node
    JOIN v1_both AS rel ON start_node.id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE (start_node.id = 14)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.Person1Id, rel.Person2Id))) as path_edges
    FROM vlp_v1_path vp
    JOIN v1_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.Person1Id, rel.Person2Id))
), 
with_w3 AS (
SELECT 
      array_distinct(collect_list(CASE WHEN NOT isnull(v2.id) THEN named_struct('_1', v2.id, '_2', v2.birthday, '_3', v2.browserUsed, '_4', v2.creationDate, '_5', v2.email, '_6', v2.firstName, '_7', v2.gender, '_8', v2.id, '_9', v2.lastName, '_10', v2.locationIP, '_11', v2.speaks) END)) AS `v3`
FROM vlp_v1_path AS v1
JOIN ldbc.Person AS v0 ON v1.start_id = v0.id
JOIN ldbc.Person AS v2 ON v1.end_id = v2.id
WHERE (v0.id = 14 AND NOT v2.id = v0.id)
), 
with_w4 AS (
SELECT 
      w3.v3 AS `v3`, 
      __cg_element AS `v4`, 
      __cg_position AS `__o0`
FROM with_w3 AS w3
LATERAL VIEW posexplode(w3.v3) AS __cg_position, __cg_element
), 
v7_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.content AS `p2_v7_content__cg0`, 
      NULL AS `p2_v7_content__cg1`, 
      e.creationDate AS `p2_v7_creationDate__cg0`, 
      NULL AS `p2_v7_creationDate__cg1`, 
      e.id AS `p2_v7_id__cg0`, 
      NULL AS `p2_v7_id__cg1`, 
      NULL AS `p2_v7_imageFile`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v7_content__cg0`, 
      e.content AS `p2_v7_content__cg1`, 
      NULL AS `p2_v7_creationDate__cg0`, 
      e.creationDate AS `p2_v7_creationDate__cg1`, 
      NULL AS `p2_v7_id__cg0`, 
      e.id AS `p2_v7_id__cg1`, 
      e.imageFile AS `p2_v7_imageFile`
FROM ldbc.Post AS e
), 
v6_rels AS (
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
      (w4.v4)._8 AS `personId`, 
      (w4.v4)._6 AS `personFirstName`, 
      (w4.v4)._9 AS `personLastName`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v7.p2_v7_id__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v7.p2_v7_id__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v7.p2_v7_id__cg0, v7.p2_v7_id__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `commentOrPostId`, 
      coalesce(CASE WHEN regexp_replace(regexp_replace(typeof(v7.p2_v7_content__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v7.p2_v7_content__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v7.p2_v7_content__cg0, v7.p2_v7_content__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, v7.p2_v7_imageFile) AS `commentOrPostContent`, 
      CASE WHEN regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS `commentOrPostCreationDate`
FROM with_w4 AS w4
CROSS JOIN ldbc.Person AS v5
JOIN v6_rels AS v6 ON v6.__cg_start_0 = v5.id
JOIN v7_labels AS v7 ON v6.__cg_end_label = v7.__cg_label AND v6.__cg_end_0 = v7.__cg_id_0
WHERE (CASE WHEN regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END < 1354157106851 AND v5.id = (w4.v4)._1)
ORDER BY CASE WHEN regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v7.p2_v7_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END DESC NULLS FIRST, CASE WHEN regexp_replace(regexp_replace(typeof(v7.p2_v7_id__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v7.p2_v7_id__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v7.p2_v7_id__cg0, v7.p2_v7_id__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END ASC NULLS LAST
LIMIT 20