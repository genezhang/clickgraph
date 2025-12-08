-- LDBC SNB Interactive Complex Query 1 (IC1)
-- Friends with certain first name
--
-- Given a start Person, find Persons with a given first name that the start 
-- Person is connected to (excluding start Person) by at most 3 steps via the
-- KNOWS relationship. Return Persons, their distance from start, and some
-- additional details.

-- Parameters:
-- $personId: ID of the start person
-- $firstName: First name to search for

-- Original Cypher:
-- MATCH (:Person {id: $personId })-[path:KNOWS*1..3]-(friend:Person {firstName: $firstName })
-- WHERE friend.id <> $personId
-- WITH friend, min(size(path)) AS distance
-- ORDER BY distance ASC, friend.lastName ASC, friend.id ASC
-- LIMIT 20
-- MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City)
-- OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:Organisation)-[:IS_LOCATED_IN]->(uniCity:City)
-- ...

-- ClickGraph Cypher (simplified version focusing on core pattern):
-- Note: Full query requires multiple OPTIONAL MATCH and complex aggregations
-- This is a simplified version that demonstrates the key pattern

MATCH (p:Person {id: $personId})-[:KNOWS*1..3]-(friend:Person)
WHERE friend.firstName = $firstName AND friend.id <> $personId
WITH friend, count(*) AS cnt
RETURN 
    friend.id AS friendId,
    friend.firstName AS friendFirstName,
    friend.lastName AS friendLastName
ORDER BY friend.lastName ASC, friend.id ASC
LIMIT 20
