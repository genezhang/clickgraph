-- LDBC SNB Interactive Complex Query 2 (IC2)
-- Recent messages by your friends
--
-- Given a start Person, find the most recent Messages (Posts or Comments) 
-- from all of that Person's friends. Only consider Messages created before 
-- the given maxDate (excluding that day).

-- Parameters:
-- $personId: ID of the start person
-- $maxDate: Maximum creation date (epoch milliseconds)

-- Original Cypher:
-- MATCH (:Person {id: $personId })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)
-- WHERE message.creationDate <= $maxDate
-- RETURN
--     friend.id AS personId,
--     friend.firstName AS personFirstName,
--     friend.lastName AS personLastName,
--     message.id AS postOrCommentId,
--     coalesce(message.content,message.imageFile) AS postOrCommentContent,
--     message.creationDate AS postOrCommentCreationDate
-- ORDER BY
--     postOrCommentCreationDate DESC,
--     toInteger(postOrCommentId) ASC
-- LIMIT 20

-- ClickGraph version (using Post with HAS_CREATOR relationship):
MATCH (p:Person {id: $personId})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)
WHERE post.creationDate <= $maxDate
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    post.id AS postId,
    post.content AS postContent,
    post.creationDate AS postCreationDate
ORDER BY postCreationDate DESC, post.id ASC
LIMIT 20
