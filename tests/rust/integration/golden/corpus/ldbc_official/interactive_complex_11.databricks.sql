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
      v2.firstName AS `p2_v3_firstName`, 
      v2.id AS `p2_v3_id`, 
      v2.lastName AS `p2_v3_lastName`
FROM vlp_v1_path AS v1
JOIN ldbc.Person AS v0 ON v1.start_id = v0.id
JOIN ldbc.Person AS v2 ON v1.end_id = v2.id
WHERE (v0.id = 14 AND NOT v0.id = v2.id)
)
SELECT 
      w3.p2_v3_id AS `personId`, 
      w3.p2_v3_firstName AS `personFirstName`, 
      w3.p2_v3_lastName AS `personLastName`, 
      v5.name AS `organizationName`, 
      v4.workFrom AS `organizationWorkFromYear`
FROM with_w3 AS w3
JOIN ldbc.Person_workAt_Organisation AS v4 ON v4.PersonId = w3.v3__id
JOIN ldbc.Organisation AS v5 ON v4.CompanyId = v5.id
JOIN ldbc.Organisation_isLocatedIn_Place AS v6 ON v6.OrganisationId = v5.id
JOIN ldbc.Place AS v7 ON v6.PlaceId = v7.id
WHERE (((v5.type = 'Company')) AND ((v7.type = 'Country')) AND v7.name = 'India' AND v4.workFrom < 2010)
ORDER BY v4.workFrom ASC NULLS LAST, bigint(w3.p2_v3_id) ASC NULLS LAST, v5.name DESC NULLS FIRST
LIMIT 10