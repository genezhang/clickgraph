WITH RECURSIVE v2_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      e.content AS "p2_v2_content__cg0", 
      NULL AS "p2_v2_content__cg1", 
      e.creationDate AS "p2_v2_creationDate__cg0", 
      NULL AS "p2_v2_creationDate__cg1", 
      e.id AS "p2_v2_id__cg0", 
      NULL AS "p2_v2_id__cg1", 
      NULL AS "p2_v2_imageFile"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      NULL AS "p2_v2_content__cg0", 
      e.content AS "p2_v2_content__cg1", 
      NULL AS "p2_v2_creationDate__cg0", 
      e.creationDate AS "p2_v2_creationDate__cg1", 
      NULL AS "p2_v2_id__cg0", 
      e.id AS "p2_v2_id__cg1", 
      e.imageFile AS "p2_v2_imageFile"
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
), 
with_w3 AS (
SELECT 
      v2.__cg_label AS "v3____cg_label", 
      v2.__cg_id_0 AS "v3____cg_id_0", 
      CAST(coalesce(v2.p2_v2_content__cg0, v2.p2_v2_content__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_content__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_content__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_content__cg0, v2.p2_v2_content__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_content__cg0, v2.p2_v2_content__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "p2_v3_content", 
      v2.p2_v2_imageFile AS "p2_v3_imageFile", 
      CAST(coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "v4", 
      CAST(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "v5", 
      CAST(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "__o0", 
      CAST(coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "__o1"
FROM ldbc.Person AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
WHERE v0.id = 14
ORDER BY CAST(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) DESC NULLS FIRST, CAST(coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_id__cg0, v2.p2_v2_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) ASC, tuple(multiIf(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'None', '', replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'Bool', 'b', match(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(U?Int[0-9]|Float|BFloat|Decimal)'), 'n', match(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(Array|Map|Tuple)'), 'c', match(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(String|FixedString|Enum|UUID|IPv|LowCardinality)'), 's', replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'Date32', 'Date', startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), concat('DateTime ', if(extract(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '''([^'']*)''') = '', timezone(), extract(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '''([^'']*)'''))), startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'Time'), 'Time', replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1')), multiIf(match(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(U?Int[0-9]|Float|BFloat|Decimal)') AND toString("v3____cg_label") = '-0', '0', startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'FixedString'), rightPad(toString("v3____cg_label"), toUInt64OrZero(extract(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '[0-9]+')), '\0'), startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), concat(replaceRegexpOne(replaceRegexpOne(toString("v3____cg_label"), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), ' ', ifNull(toString(toUnixTimestamp64Micro(accurateCastOrNull(if(startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), CAST("v3____cg_label", 'Dynamic'), NULL), 'DateTime64(6, ''UTC'')'))), '')), startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_label", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'Time'), replaceRegexpOne(replaceRegexpOne(toString("v3____cg_label"), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), toString("v3____cg_label"))) ASC, tuple(multiIf(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'None', '', replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'Bool', 'b', match(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(U?Int[0-9]|Float|BFloat|Decimal)'), 'n', match(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(Array|Map|Tuple)'), 'c', match(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(String|FixedString|Enum|UUID|IPv|LowCardinality)'), 's', replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'Date32', 'Date', startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), concat('DateTime ', if(extract(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '''([^'']*)''') = '', timezone(), extract(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '''([^'']*)'''))), startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'Time'), 'Time', replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1')), multiIf(match(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(U?Int[0-9]|Float|BFloat|Decimal)') AND toString("v3____cg_id_0") = '-0', '0', startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'FixedString'), rightPad(toString("v3____cg_id_0"), toUInt64OrZero(extract(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '[0-9]+')), '\0'), startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), concat(replaceRegexpOne(replaceRegexpOne(toString("v3____cg_id_0"), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), ' ', ifNull(toString(toUnixTimestamp64Micro(accurateCastOrNull(if(startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), CAST("v3____cg_id_0", 'Dynamic'), NULL), 'DateTime64(6, ''UTC'')'))), '')), startsWith(replaceRegexpOne(dynamicType(CAST("v3____cg_id_0", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'Time'), replaceRegexpOne(replaceRegexpOne(toString("v3____cg_id_0"), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), toString("v3____cg_id_0"))) ASC, tuple(multiIf(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'None', '', replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'Bool', 'b', match(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(U?Int[0-9]|Float|BFloat|Decimal)'), 'n', match(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(Array|Map|Tuple)'), 'c', match(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(String|FixedString|Enum|UUID|IPv|LowCardinality)'), 's', replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'Date32', 'Date', startsWith(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), concat('DateTime ', if(extract(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '''([^'']*)''') = '', timezone(), extract(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '''([^'']*)'''))), startsWith(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'Time'), 'Time', replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1')), multiIf(match(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(U?Int[0-9]|Float|BFloat|Decimal)') AND toString("v4") = '-0', '0', startsWith(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'FixedString'), rightPad(toString("v4"), toUInt64OrZero(extract(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '[0-9]+')), '\0'), startsWith(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), concat(replaceRegexpOne(replaceRegexpOne(toString("v4"), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), ' ', ifNull(toString(toUnixTimestamp64Micro(accurateCastOrNull(if(startsWith(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), CAST("v4", 'Dynamic'), NULL), 'DateTime64(6, ''UTC'')'))), '')), startsWith(replaceRegexpOne(dynamicType(CAST("v4", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'Time'), replaceRegexpOne(replaceRegexpOne(toString("v4"), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), toString("v4"))) ASC, tuple(multiIf(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'None', '', replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'Bool', 'b', match(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(U?Int[0-9]|Float|BFloat|Decimal)'), 'n', match(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(Array|Map|Tuple)'), 'c', match(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(String|FixedString|Enum|UUID|IPv|LowCardinality)'), 's', replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'Date32', 'Date', startsWith(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), concat('DateTime ', if(extract(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '''([^'']*)''') = '', timezone(), extract(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '''([^'']*)'''))), startsWith(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'Time'), 'Time', replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1')), multiIf(match(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '^(U?Int[0-9]|Float|BFloat|Decimal)') AND toString("v5") = '-0', '0', startsWith(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'FixedString'), rightPad(toString("v5"), toUInt64OrZero(extract(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), '[0-9]+')), '\0'), startsWith(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), concat(replaceRegexpOne(replaceRegexpOne(toString("v5"), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), ' ', ifNull(toString(toUnixTimestamp64Micro(accurateCastOrNull(if(startsWith(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'DateTime'), CAST("v5", 'Dynamic'), NULL), 'DateTime64(6, ''UTC'')'))), '')), startsWith(replaceRegexpOne(dynamicType(CAST("v5", 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1'), 'Time'), replaceRegexpOne(replaceRegexpOne(toString("v5"), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), toString("v5"))) ASC
LIMIT 10), 
vlp_v6_nodes AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      concat('Comment', ':', toString(e.id)) AS "__cg_key"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      concat('Post', ':', toString(e.id)) AS "__cg_key"
FROM ldbc.Post AS e
), 
v6_rels AS (
SELECT 
      'REPLY_OF' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Comment' AS "__cg_to_label", 
      e.Comment1Id AS "__cg_rid_0", 
      e.Comment2Id AS "__cg_rid_1", 
      e.Comment1Id AS "__cg_from_0", 
      e.Comment2Id AS "__cg_to_0", 
      e.Comment1Id AS "__cg_start_0", 
      e.Comment2Id AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('REPLY_OF:Comment:Comment:', toString(tuple(e.Comment1Id, e.Comment2Id))) AS "__cg_key"
FROM ldbc.Comment_replyOf_Comment AS e

UNION ALL
SELECT 
      'REPLY_OF' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PostId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PostId AS "__cg_to_0", 
      e.CommentId AS "__cg_start_0", 
      e.PostId AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('REPLY_OF:Comment:Post:', toString(tuple(e.CommentId, e.PostId))) AS "__cg_key"
FROM ldbc.Comment_replyOf_Post AS e
), 
vlp_v6_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes
    FROM vlp_v6_nodes AS start_node
    WHERE ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      w3.v3____cg_label AS "label", 
      w3.v3____cg_id_0 AS "id"
FROM with_w3 AS w3))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS UInt32) AS hop_count, arrayConcat(vp.path_edges, [rel.__cg_key]) AS path_edges, arrayConcat(vp.path_nodes, [end_node.__cg_key]) AS path_nodes
    FROM vlp_v6_trails AS vp
    JOIN v6_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v6_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE NOT has(vp.path_edges, rel.__cg_key)
), 
vlp_v6_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v6_trails
)
SELECT 
      w3.v4 AS "messageId", 
      coalesce(w3.p2_v3_imageFile, w3.p2_v3_content) AS "messageContent", 
      w3.v5 AS "messageCreationDate", 
      v7.id AS "postId", 
      v9.id AS "personId", 
      v9.firstName AS "personFirstName", 
      v9.lastName AS "personLastName"
FROM vlp_v6_path AS v6
JOIN with_w3 AS w3 ON v6.start_label = w3.v3____cg_label AND v6.start_id = w3.v3____cg_id_0
JOIN ldbc.Post AS v7 ON v6.end_id = v7.id
JOIN ldbc.Post_hasCreator_Person AS v8 ON v8.PostId = v7.id
JOIN ldbc.Person AS v9 ON v8.PersonId = v9.id
WHERE v6.end_label = 'Post'
ORDER BY w3.v5 DESC NULLS FIRST, w3.v4 ASC

-- column messageId: Value [("messageId", "messageId")]
-- column messageContent: Value [("messageContent", "messageContent")]
-- column messageCreationDate: Value [("messageCreationDate", "messageCreationDate")]
-- column postId: Value [("postId", "postId")]
-- column personId: Value [("personId", "personId")]
-- column personFirstName: Value [("personFirstName", "personFirstName")]
-- column personLastName: Value [("personLastName", "personLastName")]