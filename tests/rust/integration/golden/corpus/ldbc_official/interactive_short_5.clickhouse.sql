WITH v0_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      e.id AS "p2_v0_id__cg0", 
      NULL AS "p2_v0_id__cg1"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      NULL AS "p2_v0_id__cg0", 
      e.id AS "p2_v0_id__cg1"
FROM ldbc.Post AS e
), 
v1_rels AS (
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.CommentId AS "__cg_start_0", 
      e.PersonId AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Person' AS "__cg_end_label", 
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
      e.PostId AS "__cg_start_0", 
      e.PersonId AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'Person' AS "__cg_end_label", 
      concat('HAS_CREATOR:Post:Person:', toString(tuple(e.PostId, e.PersonId))) AS "__cg_key"
FROM ldbc.Post_hasCreator_Person AS e
)
SELECT 
      v2.id AS "personId", 
      v2.firstName AS "firstName", 
      v2.lastName AS "lastName"
FROM v0_labels AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_label = v0.__cg_label AND v1.__cg_start_0 = v0.__cg_id_0
JOIN ldbc.Person AS v2 ON v1.__cg_end_0 = v2.id
WHERE CAST(coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) = 3
