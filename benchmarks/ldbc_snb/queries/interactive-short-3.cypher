-- LDBC SNB Interactive Short Query 3 (IS3)
-- Friends of a person
--
-- Given a start Person, retrieve all of their friends, and the date at 
-- which they became friends.

-- Parameters:
-- $personId: ID of the person

-- Original Cypher:
-- MATCH (n:Person {id: $personId })-[r:KNOWS]-(friend)
-- RETURN
--     friend.id AS personId,
--     friend.firstName AS firstName,
--     friend.lastName AS lastName,
--     r.creationDate AS friendshipCreationDate
-- ORDER BY friendshipCreationDate DESC, personId ASC

-- ClickGraph version:
MATCH (n:Person {id: $personId})-[r:KNOWS]-(friend:Person)
RETURN
    friend.id AS personId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    r.creationDate AS friendshipCreationDate
ORDER BY r.creationDate DESC, friend.id ASC
