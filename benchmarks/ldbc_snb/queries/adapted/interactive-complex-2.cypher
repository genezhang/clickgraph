


























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
