WITH RECURSIVE with_w1 AS (
SELECT 
      v0.id AS "v0__id"
FROM ldbc.Person AS v0
), 
optional_d2 AS (
SELECT DISTINCT 
      w1.v0__id AS "v0__id"
FROM with_w1 AS w1
), 
v2_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      e.content AS "p2_v2_content__cg0", 
      NULL AS "p2_v2_content__cg1", 
      e.creationDate AS "p2_v2_creationDate__cg0", 
      NULL AS "p2_v2_creationDate__cg1", 
      e.length AS "p2_v2_length__cg0", 
      NULL AS "p2_v2_length__cg1"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      NULL AS "p2_v2_content__cg0", 
      e.content AS "p2_v2_content__cg1", 
      NULL AS "p2_v2_creationDate__cg0", 
      e.creationDate AS "p2_v2_creationDate__cg1", 
      NULL AS "p2_v2_length__cg0", 
      e.length AS "p2_v2_length__cg1"
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
vlp_v3_nodes AS (
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
v3_rels AS (
SELECT 
      'REPLY_OF' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Comment' AS "__cg_to_label", 
      e.Comment1Id AS "__cg_rid_0", 
      e.Comment2Id AS "__cg_rid_1", 
      e.Comment1Id AS "__cg_from_0", 
      e.Comment2Id AS "__cg_to_0", 
      e.Comment2Id AS "__cg_start_0", 
      e.Comment1Id AS "__cg_end_0", 
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
      e.PostId AS "__cg_start_0", 
      e.CommentId AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('REPLY_OF:Comment:Post:', toString(tuple(e.CommentId, e.PostId))) AS "__cg_key"
FROM ldbc.Comment_replyOf_Post AS e
), 
vlp_v3_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes
    FROM vlp_v3_nodes AS start_node
    WHERE (start_node.__cg_label = 'Post') AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      'Post' AS "label", 
      v4.id AS "id"
FROM ldbc.Post AS v4
WHERE v4.language IN ['en', 'fr']))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS UInt32) AS hop_count, arrayConcat(vp.path_edges, [rel.__cg_key]) AS path_edges, arrayConcat(vp.path_nodes, [end_node.__cg_key]) AS path_nodes
    FROM vlp_v3_trails AS vp
    JOIN v3_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v3_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE NOT has(vp.path_edges, rel.__cg_key)
), 
vlp_v3_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v3_trails
), 
optional_o3 AS (
SELECT 
      d2.v0__id AS "v0__id", 
      v2.__cg_label AS "v2____cg_label", 
      v2.__cg_id_0 AS "v2____cg_id_0", 
      CAST(coalesce(v2.p2_v2_content__cg0, v2.p2_v2_content__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_content__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_content__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_content__cg0, v2.p2_v2_content__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_content__cg0, v2.p2_v2_content__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "p2_v2_content", 
      CAST(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "p2_v2_creationDate", 
      CAST(coalesce(v2.p2_v2_length__cg0, v2.p2_v2_length__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_length__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_length__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_length__cg0, v2.p2_v2_length__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_length__cg0, v2.p2_v2_length__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) AS "p2_v2_length", 
      v4.id AS "v4__id", 
      v4.language AS "p2_v4_language"
FROM vlp_v3_path AS v3
JOIN v2_labels AS v2 ON v3.end_label = v2.__cg_label AND v3.end_id = v2.__cg_id_0
JOIN v1_rels AS v1 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
JOIN optional_d2 AS d2 ON v1.__cg_start_0 = d2.v0__id
JOIN ldbc.Post AS v4 ON v3.start_id = v4.id
WHERE (((CAST(coalesce(v2.p2_v2_content__cg0, v2.p2_v2_content__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_content__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_content__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_content__cg0, v2.p2_v2_content__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_content__cg0, v2.p2_v2_content__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) IS NOT NULL AND CAST(coalesce(v2.p2_v2_length__cg0, v2.p2_v2_length__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_length__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_length__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_length__cg0, v2.p2_v2_length__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_length__cg0, v2.p2_v2_length__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) < 100) AND CAST(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_creationDate__cg0, v2.p2_v2_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) > 1262305305684) AND v4.language IN ['en', 'fr'])
), 
with_w10 AS (
SELECT 
      w1.v0__id AS "v5__id", 
      count(o3.v2____cg_label) AS "v6"
FROM with_w1 AS w1
LEFT JOIN optional_o3 AS o3 ON w1.v0__id = o3.v0__id
GROUP BY w1.v0__id
)
SELECT 
      w10.v6 AS "messageCount", 
      count(w10.v5__id) AS "personCount"
FROM with_w10 AS w10
GROUP BY w10.v6
ORDER BY count(w10.v5__id) DESC NULLS FIRST, w10.v6 DESC NULLS FIRST
