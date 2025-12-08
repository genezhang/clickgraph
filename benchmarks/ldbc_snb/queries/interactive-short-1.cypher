-- LDBC SNB Interactive Short Query 1 (IS1)
-- Profile of a person
--
-- Given a start Person, retrieve their first name, last name, birthday, 
-- IP address, browser, city of residence and gender.

-- Parameters:
-- $personId: ID of the person

-- Original Cypher:
-- MATCH (n:Person {id: $personId })-[:IS_LOCATED_IN]->(p:Place)
-- RETURN
--     n.firstName AS firstName,
--     n.lastName AS lastName,
--     n.birthday AS birthday,
--     n.locationIP AS locationIP,
--     n.browserUsed AS browserUsed,
--     p.id AS cityId,
--     n.gender AS gender,
--     n.creationDate AS creationDate

-- ClickGraph version:
MATCH (n:Person {id: $personId})-[:IS_LOCATED_IN]->(city:City)
RETURN
    n.firstName AS firstName,
    n.lastName AS lastName,
    n.birthday AS birthday,
    n.locationIP AS locationIP,
    n.browserUsed AS browserUsed,
    city.id AS cityId,
    n.gender AS gender,
    n.creationDate AS creationDate
