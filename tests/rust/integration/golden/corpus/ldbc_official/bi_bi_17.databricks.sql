WITH RECURSIVE v3_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.creationDate AS `p2_v3_creationDate__cg0`, 
      NULL AS `p2_v3_creationDate__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p2_v3_creationDate__cg0`, 
      e.creationDate AS `p2_v3_creationDate__cg1`
FROM ldbc.Post AS e
), 
v2_rels AS (
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
), 
v8_rels AS (
SELECT 
      'HAS_TAG' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Tag' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.TagId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.TagId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.TagId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Tag' AS `__cg_end_label`, 
      concat('HAS_TAG:Comment:Tag:', to_json(named_struct('_1', CAST(e.CommentId AS STRING), '_2', CAST(e.TagId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_hasTag_Tag AS e

UNION ALL
SELECT 
      'HAS_TAG' AS `__cg_type`, 
      'Post' AS `__cg_from_label`, 
      'Tag' AS `__cg_to_label`, 
      e.PostId AS `__cg_rid_0`, 
      e.TagId AS `__cg_rid_1`, 
      e.PostId AS `__cg_from_0`, 
      e.TagId AS `__cg_to_0`, 
      e.PostId AS `__cg_start_0`, 
      e.TagId AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'Tag' AS `__cg_end_label`, 
      concat('HAS_TAG:Post:Tag:', to_json(named_struct('_1', CAST(e.PostId AS STRING), '_2', CAST(e.TagId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Post_hasTag_Tag AS e
), 
v17_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.creationDate AS `p3_v17_creationDate__cg0`, 
      NULL AS `p3_v17_creationDate__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p3_v17_creationDate__cg0`, 
      e.creationDate AS `p3_v17_creationDate__cg1`
FROM ldbc.Post AS e
), 
v16_rels AS (
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
), 
v18_rels AS (
SELECT 
      'REPLY_OF' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Comment' AS `__cg_to_label`, 
      e.Comment1Id AS `__cg_rid_0`, 
      e.Comment2Id AS `__cg_rid_1`, 
      e.Comment1Id AS `__cg_from_0`, 
      e.Comment2Id AS `__cg_to_0`, 
      e.Comment1Id AS `__cg_start_0`, 
      e.Comment2Id AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Comment' AS `__cg_end_label`, 
      concat('REPLY_OF:Comment:Comment:', to_json(named_struct('_1', CAST(e.Comment1Id AS STRING), '_2', CAST(e.Comment2Id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_replyOf_Comment AS e

UNION ALL
SELECT 
      'REPLY_OF' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.PostId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.PostId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.PostId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('REPLY_OF:Comment:Post:', to_json(named_struct('_1', CAST(e.CommentId AS STRING), '_2', CAST(e.PostId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_replyOf_Post AS e
), 
vlp_v4_nodes AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      concat('Comment', ':', CAST(e.id AS STRING)) AS `__cg_key`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      concat('Post', ':', CAST(e.id AS STRING)) AS `__cg_key`
FROM ldbc.Post AS e
), 
v4_rels AS (
SELECT 
      'REPLY_OF' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Comment' AS `__cg_to_label`, 
      e.Comment1Id AS `__cg_rid_0`, 
      e.Comment2Id AS `__cg_rid_1`, 
      e.Comment1Id AS `__cg_from_0`, 
      e.Comment2Id AS `__cg_to_0`, 
      e.Comment1Id AS `__cg_start_0`, 
      e.Comment2Id AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Comment' AS `__cg_end_label`, 
      concat('REPLY_OF:Comment:Comment:', to_json(named_struct('_1', CAST(e.Comment1Id AS STRING), '_2', CAST(e.Comment2Id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_replyOf_Comment AS e

UNION ALL
SELECT 
      'REPLY_OF' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.PostId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.PostId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.PostId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('REPLY_OF:Comment:Post:', to_json(named_struct('_1', CAST(e.CommentId AS STRING), '_2', CAST(e.PostId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_replyOf_Post AS e
), 
vlp_v4_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes
    FROM vlp_v4_nodes AS start_node
    WHERE ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      v3.__cg_label AS `label`, 
      v3.__cg_id_0 AS `id`
FROM ldbc.Person AS v1
JOIN v2_rels AS v2 ON v2.__cg_start_0 = v1.id
JOIN v3_labels AS v3 ON v2.__cg_end_label = v3.__cg_label AND v2.__cg_end_0 = v3.__cg_id_0))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes
    FROM vlp_v4_trails AS vp
    JOIN v4_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v4_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v4_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v4_trails
), 
vlp_v19_nodes AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      concat('Comment', ':', CAST(e.id AS STRING)) AS `__cg_key`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      concat('Post', ':', CAST(e.id AS STRING)) AS `__cg_key`
FROM ldbc.Post AS e
), 
v19_rels AS (
SELECT 
      'REPLY_OF' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Comment' AS `__cg_to_label`, 
      e.Comment1Id AS `__cg_rid_0`, 
      e.Comment2Id AS `__cg_rid_1`, 
      e.Comment1Id AS `__cg_from_0`, 
      e.Comment2Id AS `__cg_to_0`, 
      e.Comment1Id AS `__cg_start_0`, 
      e.Comment2Id AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Comment' AS `__cg_end_label`, 
      concat('REPLY_OF:Comment:Comment:', to_json(named_struct('_1', CAST(e.Comment1Id AS STRING), '_2', CAST(e.Comment2Id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_replyOf_Comment AS e

UNION ALL
SELECT 
      'REPLY_OF' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.PostId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.PostId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.PostId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('REPLY_OF:Comment:Post:', to_json(named_struct('_1', CAST(e.CommentId AS STRING), '_2', CAST(e.PostId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_replyOf_Post AS e
), 
vlp_v19_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes
    FROM vlp_v19_nodes AS start_node
    WHERE ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      v17.__cg_label AS `label`, 
      v17.__cg_id_0 AS `id`
FROM ldbc.Tag AS v0
CROSS JOIN ldbc.Person AS v1
JOIN v2_rels AS v2 ON v2.__cg_start_0 = v1.id
JOIN v3_labels AS v3 ON v2.__cg_end_label = v3.__cg_label AND v2.__cg_end_0 = v3.__cg_id_0
JOIN vlp_v4_path AS v4 ON v4.start_label = v3.__cg_label AND v4.start_id = v3.__cg_id_0
JOIN ldbc.Post AS v5 ON v4.end_id = v5.id
JOIN ldbc.Forum_containerOf_Post AS v6 ON v6.PostId = v5.id
JOIN ldbc.Forum AS v7 ON v6.ForumId = v7.id
JOIN v8_rels AS v8 ON v8.__cg_start_label = v3.__cg_label AND v8.__cg_start_0 = v3.__cg_id_0 AND v8.__cg_end_0 = v0.id
JOIN ldbc.Forum_hasMember_Person AS v9 ON v9.ForumId = v7.id
JOIN ldbc.Person AS v10 ON v9.PersonId = v10.id
JOIN ldbc.Comment_hasCreator_Person AS v11 ON v11.PersonId = v10.id
JOIN ldbc.Comment AS v12 ON v11.CommentId = v12.id
JOIN ldbc.Comment_hasTag_Tag AS v13 ON v13.CommentId = v12.id AND v13.TagId = v0.id
JOIN ldbc.Forum_hasMember_Person AS v14 ON v14.ForumId = v7.id
JOIN ldbc.Person AS v15 ON v14.PersonId = v15.id
JOIN v16_rels AS v16 ON v16.__cg_start_0 = v15.id
JOIN v17_labels AS v17 ON v16.__cg_end_label = v17.__cg_label AND v16.__cg_end_0 = v17.__cg_id_0
JOIN v18_rels AS v18 ON v18.__cg_start_0 = v12.id AND v18.__cg_end_label = v17.__cg_label AND v18.__cg_end_0 = v17.__cg_id_0
WHERE (v4.end_label = 'Post' AND v0.name = 'Hamid_Karzai')))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes
    FROM vlp_v19_trails AS vp
    JOIN v19_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v19_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v19_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v19_trails
), 
v24_rels AS (
SELECT 
      'HAS_TAG' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Tag' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.TagId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.TagId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.TagId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Tag' AS `__cg_end_label`, 
      concat('HAS_TAG:Comment:Tag:', to_json(named_struct('_1', CAST(e.CommentId AS STRING), '_2', CAST(e.TagId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_hasTag_Tag AS e

UNION ALL
SELECT 
      'HAS_TAG' AS `__cg_type`, 
      'Post' AS `__cg_from_label`, 
      'Tag' AS `__cg_to_label`, 
      e.PostId AS `__cg_rid_0`, 
      e.TagId AS `__cg_rid_1`, 
      e.PostId AS `__cg_from_0`, 
      e.TagId AS `__cg_to_0`, 
      e.PostId AS `__cg_start_0`, 
      e.TagId AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'Tag' AS `__cg_end_label`, 
      concat('HAS_TAG:Post:Tag:', to_json(named_struct('_1', CAST(e.PostId AS STRING), '_2', CAST(e.TagId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Post_hasTag_Tag AS e
), 
subquery_s16 AS (
SELECT 
      v25.ForumId AS `v22__id`, 
      v25.PersonId AS `v1__id`, 
      CAST(count(*) > 0 AS BOOLEAN) AS `__value`
FROM ldbc.Forum_hasMember_Person AS v25
GROUP BY v25.ForumId, v25.PersonId
)
SELECT 
      v1.id AS `person1.id`, 
      count(DISTINCT CASE WHEN v17.__cg_label IS NULL THEN NULL ELSE struct(v17.__cg_label, v17.__cg_id_0) END) AS `messageCount`
FROM ldbc.Tag AS v0
CROSS JOIN ldbc.Person AS v1
JOIN v2_rels AS v2 ON v2.__cg_start_0 = v1.id
JOIN v3_labels AS v3 ON v2.__cg_end_label = v3.__cg_label AND v2.__cg_end_0 = v3.__cg_id_0
JOIN vlp_v4_path AS v4 ON v4.start_label = v3.__cg_label AND v4.start_id = v3.__cg_id_0
JOIN ldbc.Post AS v5 ON v4.end_id = v5.id
JOIN ldbc.Forum_containerOf_Post AS v6 ON v6.PostId = v5.id
JOIN ldbc.Forum AS v7 ON v6.ForumId = v7.id
JOIN v8_rels AS v8 ON v8.__cg_start_label = v3.__cg_label AND v8.__cg_start_0 = v3.__cg_id_0 AND v8.__cg_end_0 = v0.id
JOIN ldbc.Forum_hasMember_Person AS v9 ON v9.ForumId = v7.id
JOIN ldbc.Person AS v10 ON v9.PersonId = v10.id
JOIN ldbc.Comment_hasCreator_Person AS v11 ON v11.PersonId = v10.id
JOIN ldbc.Comment AS v12 ON v11.CommentId = v12.id
JOIN ldbc.Comment_hasTag_Tag AS v13 ON v13.CommentId = v12.id AND v13.TagId = v0.id
JOIN ldbc.Forum_hasMember_Person AS v14 ON v14.ForumId = v7.id
JOIN ldbc.Person AS v15 ON v14.PersonId = v15.id
JOIN v16_rels AS v16 ON v16.__cg_start_0 = v15.id
JOIN v17_labels AS v17 ON v16.__cg_end_label = v17.__cg_label AND v16.__cg_end_0 = v17.__cg_id_0
JOIN v18_rels AS v18 ON v18.__cg_start_0 = v12.id AND v18.__cg_end_label = v17.__cg_label AND v18.__cg_end_0 = v17.__cg_id_0
JOIN vlp_v19_path AS v19 ON v19.start_label = v17.__cg_label AND v19.start_id = v17.__cg_id_0
JOIN ldbc.Post AS v20 ON v19.end_id = v20.id
JOIN ldbc.Forum_containerOf_Post AS v21 ON v21.PostId = v20.id
JOIN ldbc.Forum AS v22 ON v21.ForumId = v22.id
JOIN ldbc.Comment_hasTag_Tag AS v23 ON v23.CommentId = v12.id AND v23.TagId = v0.id
JOIN v24_rels AS v24 ON v24.__cg_start_label = v17.__cg_label AND v24.__cg_start_0 = v17.__cg_id_0 AND v24.__cg_end_0 = v0.id
LEFT JOIN subquery_s16 AS s16 ON v22.id = s16.v22__id AND v1.id = s16.v1__id
WHERE (v4.end_label = 'Post' AND v19.end_label = 'Post' AND v0.name = 'Hamid_Karzai' AND (v2.__cg_type <> 'HAS_CREATOR' OR v2.__cg_from_label <> 'Comment' OR v2.__cg_to_label <> 'Person' OR v2.__cg_rid_0 <> v11.CommentId OR v2.__cg_rid_1 <> v11.PersonId) AND (v2.__cg_type <> v16.__cg_type OR v2.__cg_from_label <> v16.__cg_from_label OR v2.__cg_to_label <> v16.__cg_to_label OR v2.__cg_rid_0 <> v16.__cg_rid_0 OR v2.__cg_rid_1 <> v16.__cg_rid_1) AND NOT array_contains(v4.path_edges, v18.__cg_key) AND NOT arrays_overlap(v4.path_edges, v19.path_edges) AND (v6.ForumId <> v21.ForumId OR v6.PostId <> v21.PostId) AND (v8.__cg_type <> 'HAS_TAG' OR v8.__cg_from_label <> 'Comment' OR v8.__cg_to_label <> 'Tag' OR v8.__cg_rid_0 <> v13.CommentId OR v8.__cg_rid_1 <> v13.TagId) AND (v9.ForumId <> v14.ForumId OR v9.PersonId <> v14.PersonId) AND ('HAS_CREATOR' <> v16.__cg_type OR 'Comment' <> v16.__cg_from_label OR 'Person' <> v16.__cg_to_label OR v11.CommentId <> v16.__cg_rid_0 OR v11.PersonId <> v16.__cg_rid_1) AND NOT array_contains(v19.path_edges, v18.__cg_key) AND (v7.id <> v22.id AND CASE WHEN regexp_replace(regexp_replace(typeof(v17.p3_v17_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v17.p3_v17_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v17.p3_v17_creationDate__cg0, v17.p3_v17_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END > unix_millis(timestamp_millis(CASE WHEN regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v3.p2_v3_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v3.p2_v3_creationDate__cg0, v3.p2_v3_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END) + make_dt_interval(0, 100, 0, 0))) AND NOT coalesce(s16.__value, false))
GROUP BY v1.id
ORDER BY count(DISTINCT CASE WHEN v17.__cg_label IS NULL THEN NULL ELSE struct(v17.__cg_label, v17.__cg_id_0) END) DESC NULLS FIRST, v1.id ASC NULLS LAST
LIMIT 10
-- column person1.id: Value [("person1.id", "person1.id")]
-- column messageCount: Value [("messageCount", "messageCount")]