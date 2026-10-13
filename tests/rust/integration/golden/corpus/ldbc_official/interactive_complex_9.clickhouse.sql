WITH RECURSIVE v1_both AS (
SELECT 
      e.Person1Id AS "Person1Id", 
      e.Person2Id AS "Person2Id", 
      e.creationDate AS "creationDate", 
      e.Person1Id AS "__cg_start_0", 
      e.Person2Id AS "__cg_end_0"
FROM ldbc.Person_knows_Person AS e

UNION ALL
SELECT 
      e.Person1Id AS "Person1Id", 
      e.Person2Id AS "Person2Id", 
      e.creationDate AS "creationDate", 
      e.Person2Id AS "__cg_start_0", 
      e.Person1Id AS "__cg_end_0"
FROM ldbc.Person_knows_Person AS e
WHERE e.Person1Id <> e.Person2Id
), 
vlp_v1_path AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.id, end_node.id] as path_nodes,
        [tuple(rel.Person1Id, rel.Person2Id)] as path_edges
    FROM ldbc.Person AS start_node
    JOIN v1_both AS rel ON start_node.id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE (start_node.id = 14)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges
    FROM vlp_v1_path vp
    JOIN v1_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
), 
with_w3 AS (
SELECT 
      arrayDistinct(groupArrayIf(tuple(v2.id, v2.birthday, v2.browserUsed, v2.creationDate, v2.email, v2.firstName, v2.gender, v2.id, v2.lastName, v2.locationIP, v2.speaks), NOT isNull(v2.id))) AS "v3"
FROM vlp_v1_path AS v1
JOIN ldbc.Person AS v0 ON v1.start_id = v0.id
JOIN ldbc.Person AS v2 ON v1.end_id = v2.id
WHERE (v0.id = 14 AND NOT v2.id = v0.id)
), 
with_w4 AS (
SELECT 
      w3.v3 AS "v3", 
      __cg_element AS "v4", 
      __cg_position AS "__o0"
FROM with_w3 AS w3
ARRAY JOIN w3.v3 AS __cg_element, arrayEnumerate(w3.v3) AS __cg_position
), 
v7_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      e.content AS "p2_v7_content__cg0", 
      NULL AS "p2_v7_content__cg1", 
      e.creationDate AS "p2_v7_creationDate__cg0", 
      NULL AS "p2_v7_creationDate__cg1", 
      e.id AS "p2_v7_id__cg0", 
      NULL AS "p2_v7_id__cg1", 
      NULL AS "p2_v7_imageFile"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      NULL AS "p2_v7_content__cg0", 
      e.content AS "p2_v7_content__cg1", 
      NULL AS "p2_v7_creationDate__cg0", 
      e.creationDate AS "p2_v7_creationDate__cg1", 
      NULL AS "p2_v7_id__cg0", 
      e.id AS "p2_v7_id__cg1", 
      e.imageFile AS "p2_v7_imageFile"
FROM ldbc.Post AS e
), 
v6_rels AS (
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.PersonId AS "__cg_start_0", 
      e.CommentId AS "__cg_end_0", 
      'Person' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('HAS_CREATOR:Comment:Person:', toString(tuple(e.CommentId, e.PersonId))) AS "__cg_key"
FROM ldbc.Comment_hasCreator_Person AS e

UNION ALL
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.PostId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.PostId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.PersonId AS "__cg_start_0", 
      e.PostId AS "__cg_end_0", 
      'Person' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('HAS_CREATOR:Post:Person:', toString(tuple(e.PostId, e.PersonId))) AS "__cg_key"
FROM ldbc.Post_hasCreator_Person AS e
)
SELECT 
      tupleElement(w4.v4, 8) AS "personId", 
      tupleElement(w4.v4, 6) AS "personFirstName", 
      tupleElement(w4.v4, 9) AS "personLastName", 
      CAST(coalesce(v7.p2_v7_id__cg0, v7.p2_v7_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v7.p2_v7_id__cg0, v7.p2_v7_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v7.p2_v7_id__cg0, v7.p2_v7_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "commentOrPostId", 
      coalesce(CAST(coalesce(v7.p2_v7_content__cg0, v7.p2_v7_content__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_content__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_content__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v7.p2_v7_content__cg0, v7.p2_v7_content__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v7.p2_v7_content__cg0, v7.p2_v7_content__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), v7.p2_v7_imageFile) AS "commentOrPostContent", 
      CAST(coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "commentOrPostCreationDate"
FROM with_w4 AS w4
CROSS JOIN ldbc.Person AS v5
JOIN v6_rels AS v6 ON v6.__cg_start_0 = v5.id
JOIN v7_labels AS v7 ON v6.__cg_end_label = v7.__cg_label AND v6.__cg_end_0 = v7.__cg_id_0
WHERE (CAST(coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) < 1354157106851 AND v5.id = tupleElement(w4.v4, 1))
ORDER BY CAST(coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v7.p2_v7_creationDate__cg0, v7.p2_v7_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) DESC NULLS FIRST, CAST(coalesce(v7.p2_v7_id__cg0, v7.p2_v7_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v7.p2_v7_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v7.p2_v7_id__cg0, v7.p2_v7_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v7.p2_v7_id__cg0, v7.p2_v7_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) ASC
LIMIT 20