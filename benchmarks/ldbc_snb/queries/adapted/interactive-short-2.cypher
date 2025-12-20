






























MATCH (p:Person {id: $personId})<-[:HAS_CREATOR]-(post:Post)
RETURN
    post.id AS messageId,
    post.content AS messageContent,
    post.creationDate AS messageCreationDate
ORDER BY post.creationDate DESC, post.id ASC
LIMIT 10
