WITH RECURSIVE v10_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Post AS e
), 
v11_rels AS (
SELECT 
      'HAS_TAG' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Tag' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.TagId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.TagId AS "__cg_to_0", 
      e.CommentId AS "__cg_start_0", 
      e.TagId AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Tag' AS "__cg_end_label", 
      concat('HAS_TAG:Comment:Tag:', toString(tuple(e.CommentId, e.TagId))) AS "__cg_key"
FROM ldbc.Comment_hasTag_Tag AS e

UNION ALL
SELECT 
      'HAS_TAG' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'Tag' AS "__cg_to_label", 
      e.PostId AS "__cg_rid_0", 
      e.TagId AS "__cg_rid_1", 
      e.PostId AS "__cg_from_0", 
      e.TagId AS "__cg_to_0", 
      e.PostId AS "__cg_start_0", 
      e.TagId AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'Tag' AS "__cg_end_label", 
      concat('HAS_TAG:Post:Tag:', toString(tuple(e.PostId, e.TagId))) AS "__cg_key"
FROM ldbc.Post_hasTag_Tag AS e
), 
vlp_v9_nodes AS (
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
v9_rels AS (
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
vlp_v9_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes
    FROM vlp_v9_nodes AS start_node
    WHERE (start_node.__cg_label = 'Post') AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      'Post' AS "label", 
      v8.id AS "id"
FROM ldbc.Place AS v0
JOIN ldbc.Place_isPartOf_Place AS v1 ON v1.Place2Id = v0.id
JOIN ldbc.Place AS v2 ON v1.Place1Id = v2.id
JOIN ldbc.Person_isLocatedIn_Place AS v3 ON v3.CityId = v2.id
JOIN ldbc.Person AS v4 ON v3.PersonId = v4.id
JOIN ldbc.Forum_hasModerator_Person AS v5 ON v5.PersonId = v4.id
JOIN ldbc.Forum AS v6 ON v5.ForumId = v6.id
JOIN ldbc.Forum_containerOf_Post AS v7 ON v7.ForumId = v6.id
JOIN ldbc.Post AS v8 ON v7.PostId = v8.id
WHERE v0.name = 'India'))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS UInt32) AS hop_count, arrayConcat(vp.path_edges, [rel.__cg_key]) AS path_edges, arrayConcat(vp.path_nodes, [end_node.__cg_key]) AS path_nodes
    FROM vlp_v9_trails AS vp
    JOIN v9_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v9_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE NOT has(vp.path_edges, rel.__cg_key)
), 
vlp_v9_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v9_trails
)
SELECT 
      v6.id AS "forum.id", 
      v6.title AS "forum.title", 
      v6.creationDate AS "forum.creationDate", 
      v4.id AS "person.id", 
      count(DISTINCT CASE WHEN v10.__cg_label IS NULL THEN NULL ELSE tuple(v10.__cg_label, v10.__cg_id_0) END) AS "messageCount"
FROM vlp_v9_path AS v9
JOIN ldbc.Post AS v8 ON v9.start_id = v8.id
JOIN ldbc.Forum_containerOf_Post AS v7 ON v7.PostId = v8.id
JOIN ldbc.Forum AS v6 ON v7.ForumId = v6.id
JOIN ldbc.Forum_hasModerator_Person AS v5 ON v5.ForumId = v6.id
JOIN ldbc.Person AS v4 ON v5.PersonId = v4.id
JOIN ldbc.Person_isLocatedIn_Place AS v3 ON v3.PersonId = v4.id
JOIN ldbc.Place AS v2 ON v3.CityId = v2.id
JOIN ldbc.Place_isPartOf_Place AS v1 ON v1.Place1Id = v2.id
JOIN ldbc.Place AS v0 ON v1.Place2Id = v0.id
JOIN v10_labels AS v10 ON v9.end_label = v10.__cg_label AND v9.end_id = v10.__cg_id_0
JOIN v11_rels AS v11 ON v11.__cg_start_label = v10.__cg_label AND v11.__cg_start_0 = v10.__cg_id_0
JOIN ldbc.Tag AS v12 ON v11.__cg_end_0 = v12.id
JOIN ldbc.Tag_hasType_TagClass AS v13 ON v13.TagId = v12.id
JOIN ldbc.TagClass AS v14 ON v13.TagClassId = v14.id
WHERE (((v0.type = 'Country')) AND ((v2.type = 'City')) AND v0.name = 'India' AND v14.name = 'MilitaryPerson')
GROUP BY v6.id, v6.title, v6.creationDate, v4.id
ORDER BY count(DISTINCT CASE WHEN v10.__cg_label IS NULL THEN NULL ELSE tuple(v10.__cg_label, v10.__cg_id_0) END) DESC NULLS FIRST, v6.id ASC
LIMIT 20