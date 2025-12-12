-- LDBC SNB Interactive Complex Query 9 (IC9)
-- Recent messages by friends or friends of friends
--
-- Given a start Person, find the most recent Messages created by that 
-- Person's friends or friends of friends (excluding start Person).
-- Only consider Messages created before the given maxDate.

-- Parameters:
-- $personId: ID of the start person
-- $maxDate: Maximum creation date (epoch milliseconds)

-- Original Cypher:
-- MATCH (root:Person {id: $personId })-[:KNOWS*1..2]-(friend:Person)
-- WHERE NOT friend = root
-- WITH collect(distinct friend) as friends
-- UNWIND friends as friend
--     MATCH (friend)<-[:HAS_CREATOR]-(message:Message)
--     WHERE message.creationDate < $maxDate
-- RETURN
--     friend.id AS personId,
--     friend.firstName AS personFirstName,
--     friend.lastName AS personLastName,
--     message.id AS commentOrPostId,
--     coalesce(message.content,message.imageFile) AS commentOrPostContent,
--     message.creationDate AS commentOrPostCreationDate
-- ORDER BY
--     commentOrPostCreationDate DESC,
--     message.id ASC
-- LIMIT 20

-- ClickGraph version:
MATCH (root:Person {id: $personId})-[:KNOWS*1..2]-(friend:Person)
WHERE friend.id <> $personId
WITH DISTINCT friend
MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
WHERE post.creationDate < $maxDate
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    post.id AS postId,
    post.content AS postContent,
    post.creationDate AS postCreationDate
ORDER BY postCreationDate DESC, post.id ASC
LIMIT 20
