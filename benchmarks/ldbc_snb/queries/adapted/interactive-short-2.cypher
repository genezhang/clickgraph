-- LDBC SNB Interactive Short Query 2 (IS2)
-- Recent messages of a person
--
-- Given a start Person, retrieve the last 10 Messages created by that user.
-- For each message, return that message, the original post in its 
-- conversation, and the author of that post.

-- Parameters:
-- $personId: ID of the person

-- Original Cypher:
-- MATCH (:Person {id: $personId})<-[:HAS_CREATOR]-(message)
-- WITH
--  message,
--  message.id AS messageId,
--  message.creationDate AS messageCreationDate
-- ORDER BY messageCreationDate DESC, messageId ASC
-- LIMIT 10
-- MATCH (message)-[:REPLY_OF*0..]->(post:Post),
--       (post)-[:HAS_CREATOR]->(person)
-- RETURN
--  messageId,
--  coalesce(message.imageFile,message.content) AS messageContent,
--  messageCreationDate,
--  post.id AS postId,
--  person.id AS personId,
--  person.firstName AS personFirstName,
--  person.lastName AS personLastName
-- ORDER BY messageCreationDate DESC, messageId ASC

-- ClickGraph version (simplified - just posts by person):
MATCH (p:Person {id: $personId})<-[:HAS_CREATOR]-(post:Post)
RETURN
    post.id AS messageId,
    post.content AS messageContent,
    post.creationDate AS messageCreationDate
ORDER BY post.creationDate DESC, post.id ASC
LIMIT 10
