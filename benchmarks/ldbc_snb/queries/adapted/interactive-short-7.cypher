// IS7. Replies of a message (ADAPTED for ClickGraph)
// Uses unified REPLY_OF and HAS_CREATOR relationships from schema
/*
:params { messageId: 206158432794 }
*/
MATCH (m:Message)
WHERE m.id = $messageId
MATCH (m)<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
RETURN c.id AS commentId,
    c.content AS commentContent,
    c.creationDate AS commentCreationDate,
    p.id AS replyAuthorId,
    p.firstName AS replyAuthorFirstName,
    p.lastName AS replyAuthorLastName,
    CASE
        WHEN r IS NULL THEN false
        ELSE true
    END AS replyAuthorKnowsOriginalMessageAuthor
ORDER BY commentCreationDate DESC, replyAuthorId
LIMIT 20
