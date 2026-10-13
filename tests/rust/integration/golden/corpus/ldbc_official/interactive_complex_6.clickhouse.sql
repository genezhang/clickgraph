WITH RECURSIVE with_w1 AS (
SELECT 
      v0.id AS "v1"
FROM ldbc.Tag AS v0
WHERE v0.name = 'Hamid_Karzai'
), 
v3_both AS (
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
vlp_v3_path AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.id, end_node.id] as path_nodes,
        [tuple(rel.Person1Id, rel.Person2Id)] as path_edges
    FROM ldbc.Person AS start_node
    JOIN v3_both AS rel ON start_node.id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE (start_node.id = 14)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges
    FROM vlp_v3_path vp
    JOIN v3_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
), 
with_w4 AS (
SELECT 
      w1.v1 AS "v5", 
      arrayDistinct(groupArrayIf(tuple(v4.id, v4.birthday, v4.browserUsed, v4.creationDate, v4.email, v4.firstName, v4.gender, v4.id, v4.lastName, v4.locationIP, v4.speaks), NOT isNull(v4.id))) AS "v6"
FROM vlp_v3_path AS v3
JOIN ldbc.Person AS v2 ON v3.start_id = v2.id
JOIN ldbc.Person AS v4 ON v3.end_id = v4.id
CROSS JOIN with_w1 AS w1
WHERE (v2.id = 14 AND NOT v2.id = v4.id)
GROUP BY w1.v1
), 
with_w5 AS (
SELECT 
      w4.v5 AS "v5", 
      w4.v6 AS "v6", 
      __cg_element AS "v7"
FROM with_w4 AS w4
ARRAY JOIN w4.v6 AS __cg_element
), 
with_w6 AS (
SELECT 
      v14.name AS "v15", 
      count(v10.id) AS "v16"
FROM with_w5 AS w5
CROSS JOIN ldbc.Person AS v8
JOIN ldbc.Post_hasCreator_Person AS v9 ON v9.PersonId = v8.id
JOIN ldbc.Post AS v10 ON v9.PostId = v10.id
JOIN ldbc.Post_hasTag_Tag AS v11 ON v11.PostId = v10.id
JOIN ldbc.Tag AS v12 ON v11.TagId = v12.id
JOIN ldbc.Post_hasTag_Tag AS v13 ON v13.PostId = v10.id
JOIN ldbc.Tag AS v14 ON v13.TagId = v14.id
WHERE (v12.id = w5.v5 AND (v11.PostId <> v13.PostId OR v11.TagId <> v13.TagId) AND (NOT v12.id = v14.id AND v8.id = tupleElement(w5.v7, 1)))
GROUP BY v14.name
)
SELECT 
      w6.v15 AS "tagName", 
      w6.v16 AS "postCount"
FROM with_w6 AS w6
ORDER BY w6.v16 DESC NULLS FIRST, w6.v15 ASC
LIMIT 10