WITH RECURSIVE v4_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      e.creationDate AS "p2_v4_creationDate__cg0", 
      NULL AS "p2_v4_creationDate__cg1"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      NULL AS "p2_v4_creationDate__cg0", 
      e.creationDate AS "p2_v4_creationDate__cg1"
FROM ldbc.Post AS e
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
      v2.id AS "id"
FROM ldbc.Person AS v0
JOIN ldbc.Post_hasCreator_Person AS v1 ON v1.PersonId = v0.id
JOIN ldbc.Post AS v2 ON v1.PostId = v2.id
WHERE (v2.creationDate >= 1262305305684 AND v2.creationDate <= 1354157106851)))
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
)
SELECT 
      v0.id AS "person.id", 
      v0.firstName AS "person.firstName", 
      v0.lastName AS "person.lastName", 
      count(DISTINCT v2.id) AS "threadCount", 
      count(DISTINCT CASE WHEN v4.__cg_label IS NULL THEN NULL ELSE tuple(v4.__cg_label, v4.__cg_id_0) END) AS "messageCount"
FROM vlp_v3_path AS v3
JOIN ldbc.Post AS v2 ON v3.start_id = v2.id
JOIN ldbc.Post_hasCreator_Person AS v1 ON v1.PostId = v2.id
JOIN ldbc.Person AS v0 ON v1.PersonId = v0.id
JOIN v4_labels AS v4 ON v3.end_label = v4.__cg_label AND v3.end_id = v4.__cg_id_0
WHERE (((v2.creationDate >= 1262305305684 AND v2.creationDate <= 1354157106851) AND CAST(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) >= 1262305305684) AND CAST(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_creationDate__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v4.p2_v4_creationDate__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v4.p2_v4_creationDate__cg0, v4.p2_v4_creationDate__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) <= 1354157106851)
GROUP BY v0.id, v0.firstName, v0.lastName
ORDER BY count(DISTINCT CASE WHEN v4.__cg_label IS NULL THEN NULL ELSE tuple(v4.__cg_label, v4.__cg_id_0) END) DESC NULLS FIRST, v0.id ASC
LIMIT 100