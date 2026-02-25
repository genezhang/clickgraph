// Q5. New groups (adapted)
// Same as official but with explicit node labels in OPTIONAL MATCH pattern
// to avoid multi-type expansion issues with HAS_CREATOR (Comment vs Post)
MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(otherPerson)
WHERE
    person <> otherPerson
WITH DISTINCT otherPerson
MATCH (otherPerson)<-[membership:HAS_MEMBER]-(forum:Forum)
WHERE
    membership.creationDate > $minDate
WITH
    forum,
    collect(otherPerson) AS otherPersons
OPTIONAL MATCH (otherPerson2:Person)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
WHERE
    otherPerson2 IN otherPersons
WITH
    forum,
    count(post) AS postCount
RETURN
    forum.title AS forumName,
    postCount
ORDER BY
    postCount DESC,
    forum.id ASC
LIMIT 20
