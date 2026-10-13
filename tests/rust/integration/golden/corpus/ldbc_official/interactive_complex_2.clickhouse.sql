WITH v4_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      e.content AS "p2_v4_content__cg0", 
      NULL AS "p2_v4_content__cg1", 
      e.creationDate AS "p2_v4_creationDate__cg0", 
      NULL AS "p2_v4_creationDate__cg1", 
      e.id AS "p2_v4_id__cg0", 
      NULL AS "p2_v4_id__cg1", 
      NULL AS "p2_v4_imageFile"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      NULL AS "p2_v4_content__cg0", 
      e.content AS "p2_v4_content__cg1", 
      NULL AS "p2_v4_creationDate__cg0", 
      e.creationDate AS "p2_v4_creationDate__cg1", 
      NULL AS "p2_v4_id__cg0", 
      e.id AS "p2_v4_id__cg1", 
      e.imageFile AS "p2_v4_imageFile"
FROM ldbc.Post AS e
), 
v1_both AS (
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
v3_rels AS (
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
      v2.id AS "personId", 
      v2.firstName AS "personFirstName", 
      v2.lastName AS "personLastName", 
      CAST(coalesce(v4.p2_v4_id__cg0, v4.p2_v4_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v4.p2_v4_id__cg0, v4.p2_v4_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v4.p2_v4_id__cg0, v4.p2_v4_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "postOrCommentId", 
      coalesce(CAST(coalesce(v4.p2_v4_content__cg0, v4.p2_v4_content__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_content__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_content__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v4.p2_v4_content__cg0, v4.p2_v4_content__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v4.p2_v4_content__cg0, v4.p2_v4_content__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), v4.p2_v4_imageFile) AS "postOrCommentContent", 
      CAST(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "postOrCommentCreationDate"
FROM ldbc.Person AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.id
JOIN ldbc.Person AS v2 ON v1.__cg_end_0 = v2.id
JOIN v3_rels AS v3 ON v3.__cg_start_0 = v2.id
JOIN v4_labels AS v4 ON v3.__cg_end_label = v4.__cg_label AND v3.__cg_end_0 = v4.__cg_id_0
WHERE (v0.id = 14 AND CAST(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) < 1354157106851)
ORDER BY CAST(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) DESC NULLS FIRST, toInt64(CAST(coalesce(v4.p2_v4_id__cg0, v4.p2_v4_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v4.p2_v4_id__cg0, v4.p2_v4_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v4.p2_v4_id__cg0, v4.p2_v4_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types'))) ASC
LIMIT 20
-- column personId: Value [("personId", "personId")]
-- column personFirstName: Value [("personFirstName", "personFirstName")]
-- column personLastName: Value [("personLastName", "personLastName")]
-- column postOrCommentId: Value [("postOrCommentId", "postOrCommentId")]
-- column postOrCommentContent: Value [("postOrCommentContent", "postOrCommentContent")]
-- column postOrCommentCreationDate: Value [("postOrCommentCreationDate", "postOrCommentCreationDate")]