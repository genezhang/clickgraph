// IS7. Replies of a message (ADAPTED for ClickGraph)
// Adaptation: [:REPLY_OF] → [:REPLY_OF_POST|REPLY_OF_COMMENT]
/*
:params { messageId: 206158432794 }
*/
MATCH (m:Message)
WHERE m.id = $messageId
MATCH (m)<-[:REPLY_OF_POST|REPLY_OF_COMMENT]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
OPTIONAL MATCH (m)-[:MESSAGE_HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
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
