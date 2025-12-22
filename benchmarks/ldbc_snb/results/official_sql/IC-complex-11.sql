-- LDBC Official Query: IC-complex-11
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.128778
-- Database: ldbc

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
WITH RECURSIVE vlp_cte14 AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.from_id, rel.to_id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE start_node.id = $personId
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte14 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
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
      start_node.gender AS "person.gender", 
      start_node.locationIP AS "person.locationIP", 
      start_node.birthday AS "person.birthday", 
      start_node.lastName AS "person.lastName", 
      start_node.id AS "person.id", 
      start_node.browserUsed AS "person.browserUsed", 
      start_node.firstName AS "person.firstName", 
      start_node.creationDate AS "person.creationDate", 
      end_node.gender AS "friend.gender", 
      end_node.locationIP AS "friend.locationIP", 
      end_node.birthday AS "friend.birthday", 
      end_node.lastName AS "friend.lastName", 
      end_node.id AS "friend.id", 
      end_node.browserUsed AS "friend.browserUsed", 
      end_node.firstName AS "friend.firstName", 
      end_node.creationDate AS "friend.creationDate"
FROM vlp_cte14 AS vlp14
JOIN ldbc.Person AS person ON vlp14.start_id = person.id
JOIN ldbc.Person AS friend ON vlp14.end_id = friend.id
UNION ALL 
SELECT 
      start_node.gender AS "friend.gender", 
      start_node.locationIP AS "friend.locationIP", 
      start_node.birthday AS "friend.birthday", 
      start_node.lastName AS "friend.lastName", 
      start_node.id AS "friend.id", 
      start_node.browserUsed AS "friend.browserUsed", 
      start_node.firstName AS "friend.firstName", 
      start_node.creationDate AS "friend.creationDate", 
      end_node.gender AS "person.gender", 
      end_node.locationIP AS "person.locationIP", 
      end_node.birthday AS "person.birthday", 
      end_node.lastName AS "person.lastName", 
      end_node.id AS "person.id", 
      end_node.browserUsed AS "person.browserUsed", 
      end_node.firstName AS "person.firstName", 
      end_node.creationDate AS "person.creationDate"
FROM vlp_cte15 AS vlp15
JOIN ldbc.Person AS friend ON vlp15.start_id = friend.id
JOIN ldbc.Person AS person ON vlp15.end_id = person.id
WHERE end_node.id = $personId
) AS __union
), 
vlp_cte15 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte15 AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.from_id, rel.to_id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE end_node.id = $personId
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte15 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
      AND end_node.id = $personId
)
    SELECT * FROM vlp_cte15
  )
)
SELECT 
      friend.friend_id AS "personId", 
      friend.friend_firstName AS "personFirstName", 
      friend.friend_lastName AS "personLastName", 
      company.name AS "organizationName", 
      workAt.workFrom AS "organizationWorkFromYear"
FROM with_friend_cte_1 AS friend
INNER JOIN ldbc.Person_workAt_Organisation AS workAt ON workAt.PersonId = friend.friend_id
INNER JOIN ldbc.Organisation AS company ON company.id = workAt.CompanyId
INNER JOIN ldbc.Organisation_isLocatedIn_Place AS t193 ON workAt.CompanyId = t193.OrganisationId
INNER JOIN ldbc.Organisation_isLocatedIn_Place AS t193 ON t193.OrganisationId = company.id
INNER JOIN ldbc.Place AS t192 ON t192.id = t193.PlaceId
WHERE (((t192.name = $countryName AND workAt.workFrom < $workFromYear) AND company.type = 'Company') AND t192.type = 'Country')
ORDER BY organizationWorkFromYear ASC, toInt64(personId) ASC, organizationName DESC
LIMIT  10
SETTINGS max_recursive_cte_evaluation_depth = 100

