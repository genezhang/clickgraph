-- LDBC Official Query: IC-complex-11
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.193051
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (person:Person {id: $personId })-[:KNOWS*1..2]-(friend:Person)
-- WHERE not(person=friend)
-- WITH DISTINCT friend
-- MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(:Country {name: $countryName })
-- WHERE workAt.workFrom < $workFromYear
-- RETURN
--         friend.id AS personId,
--         friend.firstName AS personFirstName,
--         friend.lastName AS personLastName,
--         company.name AS organizationName,
--         workAt.workFrom AS organizationWorkFromYear
-- ORDER BY
--         organizationWorkFromYear ASC,
--         toInteger(personId) ASC,
--         organizationName DESC
-- LIMIT 10

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte6 AS (
    SELECT 
        start_node.from_id as start_id,
        end_node.to_id as end_id,
        1 as hop_count,
        [tuple(rel.from_id, rel.to_id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.from_id, end_node.to_id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.from_id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.to_id
    WHERE start_node.id = $personId
    UNION ALL
    SELECT
        vp.start_id,
        end_node.to_id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.to_id]) as path_nodes
    FROM vlp_cte6 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.to_id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.to_id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.to_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
), 
with_friend_cte_1 AS (SELECT DISTINCT 
      friend.friend_birthday AS "friend_birthday", 
      friend.friend_browserUsed AS "friend_browserUsed", 
      friend.friend_creationDate AS "friend_creationDate", 
      friend.friend_firstName AS "friend_firstName", 
      friend.friend_gender AS "friend_gender", 
      friend.friend_id AS "friend_id", 
      friend.friend_lastName AS "friend_lastName", 
      friend.friend_locationIP AS "friend_locationIP"
FROM (
SELECT 
      person.id AS "person.id", 
      person.birthday AS "person.birthday", 
      person.firstName AS "person.firstName", 
      person.gender AS "person.gender", 
      person.locationIP AS "person.locationIP", 
      person.browserUsed AS "person.browserUsed", 
      person.lastName AS "person.lastName", 
      person.creationDate AS "person.creationDate", 
      friend.id AS "friend.id", 
      friend.birthday AS "friend.birthday", 
      friend.firstName AS "friend.firstName", 
      friend.gender AS "friend.gender", 
      friend.locationIP AS "friend.locationIP", 
      friend.browserUsed AS "friend.browserUsed", 
      friend.lastName AS "friend.lastName", 
      friend.creationDate AS "friend.creationDate"
FROM vlp_cte6 AS vlp6
JOIN ldbc.Person AS person ON vlp6.start_id = person.from_id
JOIN ldbc.Person AS friend ON vlp6.end_id = friend.to_id
UNION ALL 
SELECT 
      friend.id AS "friend.id", 
      friend.birthday AS "friend.birthday", 
      friend.firstName AS "friend.firstName", 
      friend.gender AS "friend.gender", 
      friend.locationIP AS "friend.locationIP", 
      friend.browserUsed AS "friend.browserUsed", 
      friend.lastName AS "friend.lastName", 
      friend.creationDate AS "friend.creationDate", 
      person.id AS "person.id", 
      person.birthday AS "person.birthday", 
      person.firstName AS "person.firstName", 
      person.gender AS "person.gender", 
      person.locationIP AS "person.locationIP", 
      person.browserUsed AS "person.browserUsed", 
      person.lastName AS "person.lastName", 
      person.creationDate AS "person.creationDate"
FROM vlp_cte7 AS vlp7
JOIN ldbc.Person AS friend ON vlp7.start_id = friend.from_id
JOIN ldbc.Person AS person ON vlp7.end_id = person.to_id
WHERE person.id = $personId
) AS __union
), 
vlp_cte7 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte7 AS (
    SELECT 
        start_node.from_id as start_id,
        end_node.to_id as end_id,
        1 as hop_count,
        [tuple(rel.from_id, rel.to_id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.from_id, end_node.to_id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.from_id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.to_id
    WHERE end_node.id = $personId
    UNION ALL
    SELECT
        vp.start_id,
        end_node.to_id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.to_id]) as path_nodes
    FROM vlp_cte7 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.to_id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.to_id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.to_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
      AND end_node.id = $personId
)
    SELECT * FROM vlp_cte7
  )
)
SELECT 
      start_node.friend_id AS "personId", 
      start_node.friend_firstName AS "personFirstName", 
      start_node.friend_lastName AS "personLastName", 
      company.name AS "organizationName", 
      workAt.workFrom AS "organizationWorkFromYear"
FROM with_friend_cte_1 AS friend
INNER JOIN ldbc.Person_workAt_Organisation AS workAt ON workAt.PersonId = friend.friend_id
INNER JOIN ldbc.Organisation AS company ON company.id = workAt.CompanyId
INNER JOIN ldbc.Organisation_isLocatedIn_Place AS t66 ON t66.OrganisationId = company.id
WHERE (((t65.name = $countryName AND workAt.workFrom < $workFromYear) AND (company.type = 'Company')) AND (t65.type = 'Country'))
ORDER BY organizationWorkFromYear ASC, toInt64(personId) ASC, organizationName DESC
LIMIT  10
SETTINGS max_recursive_cte_evaluation_depth = 100

