-- LDBC Official Query: IC-complex-5
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.158783
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(otherPerson)
-- WHERE
--     person <> otherPerson
-- WITH DISTINCT otherPerson
-- MATCH (otherPerson)<-[membership:HAS_MEMBER]-(forum)
-- WHERE
--     membership.creationDate > $minDate
-- WITH
--     forum,
--     collect(otherPerson) AS otherPersons
-- OPTIONAL MATCH (otherPerson2)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
-- WHERE
--     otherPerson2 IN otherPersons
-- WITH
--     forum,
--     count(post) AS postCount
-- RETURN
--     forum.title AS forumName,
--     postCount
-- ORDER BY
--     postCount DESC,
--     forum.id ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte22 AS (
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
    FROM vlp_cte22 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
), 
with_otherPerson_cte_1 AS (SELECT DISTINCT 
      otherPerson.birthday AS "otherPerson_birthday", 
      otherPerson.browserUsed AS "otherPerson_browserUsed", 
      otherPerson.creationDate AS "otherPerson_creationDate", 
      otherPerson.firstName AS "otherPerson_firstName", 
      otherPerson.gender AS "otherPerson_gender", 
      otherPerson.id AS "otherPerson_id", 
      otherPerson.lastName AS "otherPerson_lastName", 
      otherPerson.locationIP AS "otherPerson_locationIP"
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
      end_node.gender AS "otherPerson.gender", 
      end_node.locationIP AS "otherPerson.locationIP", 
      end_node.birthday AS "otherPerson.birthday", 
      end_node.lastName AS "otherPerson.lastName", 
      end_node.id AS "otherPerson.id", 
      end_node.browserUsed AS "otherPerson.browserUsed", 
      end_node.firstName AS "otherPerson.firstName", 
      end_node.creationDate AS "otherPerson.creationDate"
FROM vlp_cte22 AS vlp22
JOIN ldbc.Person AS person ON vlp22.start_id = person.id
JOIN ldbc.Person AS otherPerson ON vlp22.end_id = otherPerson.id
UNION ALL 
SELECT 
      start_node.gender AS "otherPerson.gender", 
      start_node.locationIP AS "otherPerson.locationIP", 
      start_node.birthday AS "otherPerson.birthday", 
      start_node.lastName AS "otherPerson.lastName", 
      start_node.id AS "otherPerson.id", 
      start_node.browserUsed AS "otherPerson.browserUsed", 
      start_node.firstName AS "otherPerson.firstName", 
      start_node.creationDate AS "otherPerson.creationDate", 
      end_node.gender AS "person.gender", 
      end_node.locationIP AS "person.locationIP", 
      end_node.birthday AS "person.birthday", 
      end_node.lastName AS "person.lastName", 
      end_node.id AS "person.id", 
      end_node.browserUsed AS "person.browserUsed", 
      end_node.firstName AS "person.firstName", 
      end_node.creationDate AS "person.creationDate"
FROM vlp_cte23 AS vlp23
JOIN ldbc.Person AS otherPerson ON vlp23.start_id = otherPerson.id
JOIN ldbc.Person AS person ON vlp23.end_id = person.id
WHERE end_node.id = $personId
) AS __union
), 
vlp_cte23 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte23 AS (
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
    FROM vlp_cte23 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
      AND end_node.id = $personId
)
    SELECT * FROM vlp_cte23
  )
)
SELECT 
      forum.creationDate AS "forum.creationDate", 
      forum.title AS "forum.title", 
      forum.id AS "forum.id", 
      otherPerson."otherPerson.browserUsed" AS "otherPerson.otherPerson.browserUsed", 
      otherPerson."otherPerson.id" AS "otherPerson.otherPerson.id", 
      otherPerson."person.browserUsed" AS "otherPerson.person.browserUsed", 
      otherPerson."person.id" AS "otherPerson.person.id", 
      otherPerson."person.firstName" AS "otherPerson.person.firstName", 
      otherPerson."otherPerson.locationIP" AS "otherPerson.otherPerson.locationIP", 
      otherPerson."person.lastName" AS "otherPerson.person.lastName", 
      otherPerson."otherPerson.gender" AS "otherPerson.otherPerson.gender", 
      otherPerson."person.gender" AS "otherPerson.person.gender", 
      otherPerson."otherPerson.lastName" AS "otherPerson.otherPerson.lastName", 
      otherPerson."person.birthday" AS "otherPerson.person.birthday", 
      otherPerson."otherPerson.birthday" AS "otherPerson.otherPerson.birthday", 
      otherPerson."person.creationDate" AS "otherPerson.person.creationDate", 
      otherPerson."otherPerson.firstName" AS "otherPerson.otherPerson.firstName", 
      otherPerson."person.locationIP" AS "otherPerson.person.locationIP", 
      otherPerson."otherPerson.creationDate" AS "otherPerson.otherPerson.creationDate"
FROM ldbc.Forum AS forum
INNER JOIN ldbc.Forum_hasMember_Person AS membership ON membership.ForumId = forum.id
INNER JOIN with_otherPerson_cte_1 AS otherPerson ON otherPerson.otherPerson_id = membership.PersonId
WHERE membership.creationDate > $minDate

SETTINGS max_recursive_cte_evaluation_depth = 100

