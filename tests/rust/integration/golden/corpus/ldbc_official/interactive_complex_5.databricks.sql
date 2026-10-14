WITH RECURSIVE v1_both AS (
SELECT 
      e.Person1Id AS `Person1Id`, 
      e.Person2Id AS `Person2Id`, 
      e.creationDate AS `creationDate`, 
      e.Person1Id AS `__cg_start_0`, 
      e.Person2Id AS `__cg_end_0`
FROM ldbc.Person_knows_Person AS e

UNION ALL
SELECT 
      e.Person1Id AS `Person1Id`, 
      e.Person2Id AS `Person2Id`, 
      e.creationDate AS `creationDate`, 
      e.Person2Id AS `__cg_start_0`, 
      e.Person1Id AS `__cg_end_0`
FROM ldbc.Person_knows_Person AS e
WHERE e.Person1Id <> e.Person2Id
), 
vlp_v1_path AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.id, end_node.id) as path_nodes,
        array(struct(rel.Person1Id, rel.Person2Id)) as path_edges
    FROM ldbc.Person AS start_node
    JOIN v1_both AS rel ON start_node.id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE (start_node.id = 14)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.Person1Id, rel.Person2Id))) as path_edges
    FROM vlp_v1_path vp
    JOIN v1_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.Person1Id, rel.Person2Id))
), 
with_w3 AS (
SELECT DISTINCT 
      v2.id AS `v3__id`, 
      v2.birthday AS `p2_v3_birthday`, 
      v2.browserUsed AS `p2_v3_browserUsed`, 
      v2.creationDate AS `p2_v3_creationDate`, 
      v2.email AS `p2_v3_email`, 
      v2.firstName AS `p2_v3_firstName`, 
      v2.gender AS `p2_v3_gender`, 
      v2.id AS `p2_v3_id`, 
      v2.lastName AS `p2_v3_lastName`, 
      v2.locationIP AS `p2_v3_locationIP`, 
      v2.speaks AS `p2_v3_speaks`
FROM vlp_v1_path AS v1
JOIN ldbc.Person AS v0 ON v1.start_id = v0.id
JOIN ldbc.Person AS v2 ON v1.end_id = v2.id
WHERE (v0.id = 14 AND v0.id <> v2.id)
), 
with_w4 AS (
SELECT 
      v5.id AS `v6__id`, 
      v5.id AS `p2_v6_id`, 
      v5.title AS `p2_v6_title`, 
      array_sort(collect_list(CASE WHEN NOT isnull(w3.v3__id) THEN named_struct('_1', w3.v3__id, '_2', w3.p2_v3_birthday, '_3', w3.p2_v3_browserUsed, '_4', w3.p2_v3_creationDate, '_5', w3.p2_v3_email, '_6', w3.p2_v3_firstName, '_7', w3.p2_v3_gender, '_8', w3.p2_v3_id, '_9', w3.p2_v3_lastName, '_10', w3.p2_v3_locationIP, '_11', w3.p2_v3_speaks) END), (__cg_sl, __cg_sr) -> CASE WHEN concat(typeof(__cg_sl), ':', to_json(named_struct('v', __cg_sl))) < concat(typeof(__cg_sr), ':', to_json(named_struct('v', __cg_sr))) THEN -1 WHEN concat(typeof(__cg_sl), ':', to_json(named_struct('v', __cg_sl))) > concat(typeof(__cg_sr), ':', to_json(named_struct('v', __cg_sr))) THEN 1 ELSE 0 END) AS `v7`
FROM with_w3 AS w3
JOIN ldbc.Forum_hasMember_Person AS v4 ON v4.PersonId = w3.v3__id
JOIN ldbc.Forum AS v5 ON v4.ForumId = v5.id
WHERE v4.creationDate > 1262440406808
GROUP BY v5.id, v5.id, v5.title
), 
optional_d5 AS (
SELECT DISTINCT 
      w4.v6__id AS `v6__id`, 
      w4.v7 AS `v7`
FROM with_w4 AS w4
), 
optional_o6 AS (
SELECT 
      d5.v6__id AS `v6__id`, 
      v8.id AS `v8__id`, 
      v10.id AS `v10__id`
FROM optional_d5 AS d5
JOIN ldbc.Forum_containerOf_Post AS v11 ON v11.ForumId = d5.v6__id
JOIN ldbc.Post AS v10 ON v11.PostId = v10.id
JOIN ldbc.Post_hasCreator_Person AS v9 ON v9.PostId = v10.id
JOIN ldbc.Person AS v8 ON v9.PersonId = v8.id
WHERE CAST(CASE WHEN d5.v7 IS NULL THEN NULL WHEN array_size(d5.v7) = 0 THEN false WHEN v8.id IS NULL THEN NULL ELSE array_size(filter(d5.v7, __cg_in -> (__cg_in)._1 = v8.id)) > 0 END AS BOOLEAN)
), 
with_w7 AS (
SELECT 
      w4.v6__id AS `v12__id`, 
      w4.p2_v6_id AS `p3_v12_id`, 
      w4.p2_v6_title AS `p3_v12_title`, 
      count(o6.v10__id) AS `v13`
FROM with_w4 AS w4
LEFT JOIN optional_o6 AS o6 ON w4.v6__id = o6.v6__id
GROUP BY w4.v6__id, w4.p2_v6_id, w4.p2_v6_title
)
SELECT 
      w7.p3_v12_title AS `forumName`, 
      w7.v13 AS `postCount`
FROM with_w7 AS w7
ORDER BY w7.v13 DESC NULLS FIRST, w7.p3_v12_id ASC NULLS LAST
LIMIT 20
-- column forumName: Value [("forumName", "forumName")]
-- column postCount: Value [("postCount", "postCount")]