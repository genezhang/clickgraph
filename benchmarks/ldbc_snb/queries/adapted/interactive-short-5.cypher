-- LDBC SNB Interactive Short Query 5 (IS5)
-- Creator of a message
--
-- Given a Message, retrieve its author.

-- Parameters:
-- $messageId: ID of the message (Post or Comment)

-- Original Cypher:
-- MATCH (m:Message {id: $messageId })-[:HAS_CREATOR]->(p:Person)
-- RETURN
--     p.id AS personId,
--     p.firstName AS firstName,
--     p.lastName AS lastName

-- ClickGraph version (for Posts):
MATCH (post:Post {id: $messageId})-[:HAS_CREATOR]->(p:Person)
RETURN
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName
