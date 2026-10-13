WITH RECURSIVE vlp_v1_nodes AS (
SELECT 
      'Tag' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      concat('Tag', ':', toString(e.id)) AS "__cg_key"
FROM ldbc.Tag AS e

UNION ALL
SELECT 
      'TagClass' AS "__cg_label", 
      e.id AS "__cg_id_0", 
      concat('TagClass', ':', toString(e.id)) AS "__cg_key"
FROM ldbc.TagClass AS e
), 
v1_rels AS (
SELECT 
      'HAS_TYPE' AS "__cg_type", 
      'Tag' AS "__cg_from_label", 
      'TagClass' AS "__cg_to_label", 
      e.TagId AS "__cg_rid_0", 
      e.TagClassId AS "__cg_rid_1", 
      e.TagId AS "__cg_from_0", 
      e.TagClassId AS "__cg_to_0", 
      e.TagId AS "__cg_start_0", 
      e.TagClassId AS "__cg_end_0", 
      'Tag' AS "__cg_start_label", 
      'TagClass' AS "__cg_end_label", 
      concat('HAS_TYPE:Tag:TagClass:', toString(tuple(e.TagId, e.TagClassId))) AS "__cg_key"
FROM ldbc.Tag_hasType_TagClass AS e

UNION ALL
SELECT 
      'IS_SUBCLASS_OF' AS "__cg_type", 
      'TagClass' AS "__cg_from_label", 
      'TagClass' AS "__cg_to_label", 
      e.TagClass1Id AS "__cg_rid_0", 
      e.TagClass2Id AS "__cg_rid_1", 
      e.TagClass1Id AS "__cg_from_0", 
      e.TagClass2Id AS "__cg_to_0", 
      e.TagClass1Id AS "__cg_start_0", 
      e.TagClass2Id AS "__cg_end_0", 
      'TagClass' AS "__cg_start_label", 
      'TagClass' AS "__cg_end_label", 
      concat('IS_SUBCLASS_OF:TagClass:TagClass:', toString(tuple(e.TagClass1Id, e.TagClass2Id))) AS "__cg_key"
FROM ldbc.TagClass_isSubclassOf_TagClass AS e
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes
    FROM vlp_v1_nodes AS start_node
    WHERE (start_node.__cg_label = 'Tag')
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
), 
with_w5 AS (
SELECT 
      coalesce(groupArray(v0.id), []) AS "v3"
FROM vlp_v1_path AS v1
JOIN ldbc.Tag AS v0 ON v1.start_id = v0.id
JOIN ldbc.TagClass AS v2 ON v1.end_id = v2.id
WHERE (v1.end_label = 'TagClass' AND (v0.name = 'MilitaryPerson' OR v2.name = 'MilitaryPerson'))
), 
v5_both AS (
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
)
SELECT 
      v6.id AS "personId", 
      v6.firstName AS "personFirstName", 
      v6.lastName AS "personLastName", 
      coalesce(groupArray(DISTINCT v12.name), []) AS "tagNames", 
      count(DISTINCT v8.id) AS "replyCount"
FROM with_w5 AS w5
CROSS JOIN ldbc.Person AS v4
JOIN v5_both AS v5 ON v5.__cg_start_0 = v4.id
JOIN ldbc.Person AS v6 ON v5.__cg_end_0 = v6.id
JOIN ldbc.Comment_hasCreator_Person AS v7 ON v7.PersonId = v6.id
JOIN ldbc.Comment AS v8 ON v7.CommentId = v8.id
JOIN ldbc.Comment_replyOf_Post AS v9 ON v9.CommentId = v8.id
JOIN ldbc.Post AS v10 ON v9.PostId = v10.id
JOIN ldbc.Post_hasTag_Tag AS v11 ON v11.PostId = v10.id
JOIN ldbc.Tag AS v12 ON v11.TagId = v12.id
WHERE (v4.id = 14 AND has(w5.v3, v12.id))
GROUP BY v6.id, v6.firstName, v6.lastName
ORDER BY count(DISTINCT v8.id) DESC NULLS FIRST, toInt64(v6.id) ASC
LIMIT 20