WITH RECURSIVE v0_labels AS (
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
vlp_v1_nodes AS (
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
v1_rels AS (
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
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes
    FROM vlp_v1_nodes AS start_node
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS UInt32) AS hop_count, arrayConcat(vp.path_edges, [rel.__cg_key]) AS path_edges, arrayConcat(vp.path_nodes, [end_node.__cg_key]) AS path_nodes
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE NOT has(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v1_trails
)
SELECT 
      v4.id AS "forumId", 
      v4.title AS "forumTitle", 
      v6.id AS "moderatorId", 
      v6.firstName AS "moderatorFirstName", 
      v6.lastName AS "moderatorLastName"
FROM vlp_v1_path AS v1
JOIN v0_labels AS v0 ON v1.start_label = v0.__cg_label AND v1.start_id = v0.__cg_id_0
JOIN ldbc.Post AS v2 ON v1.end_id = v2.id
JOIN ldbc.Forum_containerOf_Post AS v3 ON v3.PostId = v2.id
JOIN ldbc.Forum AS v4 ON v3.ForumId = v4.id
JOIN ldbc.Forum_hasModerator_Person AS v5 ON v5.ForumId = v4.id
JOIN ldbc.Person AS v6 ON v5.PersonId = v6.id
WHERE (v1.end_label = 'Post' AND CAST(coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v0.p2_v0_id__cg0, v0.p2_v0_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')) = 3)
