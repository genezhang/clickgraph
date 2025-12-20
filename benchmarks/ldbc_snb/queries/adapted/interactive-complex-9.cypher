






























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
