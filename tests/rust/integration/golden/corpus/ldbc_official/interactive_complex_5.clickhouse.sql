WITH RECURSIVE v1_both AS (
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
vlp_v1_path AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.id, end_node.id] as path_nodes,
        [tuple(rel.Person1Id, rel.Person2Id)] as path_edges
    FROM ldbc.Person AS start_node
    JOIN v1_both AS rel ON start_node.id = rel.__cg_start_0
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
    FROM vlp_v1_path vp
    JOIN v1_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
), 
with_w3 AS (
SELECT DISTINCT 
      v2.id AS "v3__id", 
      v2.birthday AS "p2_v3_birthday", 
      v2.browserUsed AS "p2_v3_browserUsed", 
      v2.creationDate AS "p2_v3_creationDate", 
      v2.email AS "p2_v3_email", 
      v2.firstName AS "p2_v3_firstName", 
      v2.gender AS "p2_v3_gender", 
      v2.id AS "p2_v3_id", 
      v2.lastName AS "p2_v3_lastName", 
      v2.locationIP AS "p2_v3_locationIP", 
      v2.speaks AS "p2_v3_speaks"
FROM vlp_v1_path AS v1
JOIN ldbc.Person AS v0 ON v1.start_id = v0.id
JOIN ldbc.Person AS v2 ON v1.end_id = v2.id
WHERE (v0.id = 14 AND v0.id <> v2.id)
), 
with_w4 AS (
SELECT 
      v5.id AS "v6__id", 
      v5.id AS "p2_v6_id", 
      v5.title AS "p2_v6_title", 
      arraySort(groupArrayIf(tuple(w3.v3__id, w3.p2_v3_birthday, w3.p2_v3_browserUsed, w3.p2_v3_creationDate, w3.p2_v3_email, w3.p2_v3_firstName, w3.p2_v3_gender, w3.p2_v3_id, w3.p2_v3_lastName, w3.p2_v3_locationIP, w3.p2_v3_speaks), NOT isNull(w3.v3__id))) AS "v7"
FROM with_w3 AS w3
JOIN ldbc.Forum_hasMember_Person AS v4 ON v4.PersonId = w3.v3__id
JOIN ldbc.Forum AS v5 ON v4.ForumId = v5.id
WHERE v4.creationDate > 1262440406808
GROUP BY v5.id, v5.id, v5.title
), 
optional_d5 AS (
SELECT DISTINCT 
      w4.v6__id AS "v6__id", 
      w4.v7 AS "v7"
FROM with_w4 AS w4
), 
optional_o6 AS (
SELECT 
      d5.v6__id AS "v6__id", 
      v8.id AS "v8__id", 
      v10.id AS "v10__id"
FROM optional_d5 AS d5
JOIN ldbc.Forum_containerOf_Post AS v11 ON v11.ForumId = d5.v6__id
JOIN ldbc.Post AS v10 ON v11.PostId = v10.id
JOIN ldbc.Post_hasCreator_Person AS v9 ON v9.PostId = v10.id
JOIN ldbc.Person AS v8 ON v9.PersonId = v8.id
WHERE CAST(CASE WHEN d5.v7 IS NULL THEN NULL WHEN length(d5.v7) = 0 THEN false WHEN v8.id IS NULL THEN NULL ELSE length(arrayFilter(__cg_in -> tupleElement(__cg_in, 1) = v8.id, d5.v7)) > 0 END AS Nullable(Bool))
), 
with_w7 AS (
SELECT 
      w4.v6__id AS "v12__id", 
      w4.p2_v6_id AS "p3_v12_id", 
      w4.p2_v6_title AS "p3_v12_title", 
      count(o6.v10__id) AS "v13"
FROM with_w4 AS w4
LEFT JOIN optional_o6 AS o6 ON w4.v6__id = o6.v6__id
GROUP BY w4.v6__id, w4.p2_v6_id, w4.p2_v6_title
)
SELECT 
      w7.p3_v12_title AS "forumName", 
      w7.v13 AS "postCount"
FROM with_w7 AS w7
ORDER BY w7.v13 DESC NULLS FIRST, w7.p3_v12_id ASC
LIMIT 20
-- column forumName: Value [("forumName", "forumName")]
-- column postCount: Value [("postCount", "postCount")]