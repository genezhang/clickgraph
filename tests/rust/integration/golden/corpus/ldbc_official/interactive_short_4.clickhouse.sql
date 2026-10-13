WITH v0_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      e.content AS "p2_v0_content__cg0", 
      NULL AS "p2_v0_content__cg1", 
      e.creationDate AS "p2_v0_creationDate__cg0", 
      NULL AS "p2_v0_creationDate__cg1", 
      e.id AS "p2_v0_id__cg0", 
      NULL AS "p2_v0_id__cg1", 
      NULL AS "p2_v0_imageFile"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      NULL AS "p2_v0_content__cg0", 
      e.content AS "p2_v0_content__cg1", 
      NULL AS "p2_v0_creationDate__cg0", 
      e.creationDate AS "p2_v0_creationDate__cg1", 
      NULL AS "p2_v0_id__cg0", 
      e.id AS "p2_v0_id__cg1", 
      e.imageFile AS "p2_v0_imageFile"
FROM ldbc.Post AS e
)
SELECT 
      CAST(coalesce(v0.p2_v0_creationDate__cg0, v0.p2_v0_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v0.p2_v0_creationDate__cg0, v0.p2_v0_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v0.p2_v0_creationDate__cg0, v0.p2_v0_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "messageCreationDate", 
      coalesce(CAST(coalesce(v0.p2_v0_content__cg0, v0.p2_v0_content__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_content__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_content__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v0.p2_v0_content__cg0, v0.p2_v0_content__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v0.p2_v0_content__cg0, v0.p2_v0_content__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), v0.p2_v0_imageFile) AS "messageContent"
FROM v0_labels AS v0
WHERE CAST(coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) = 3

-- column messageCreationDate: Value [("messageCreationDate", "messageCreationDate")]
-- column messageContent: Value [("messageContent", "messageContent")]