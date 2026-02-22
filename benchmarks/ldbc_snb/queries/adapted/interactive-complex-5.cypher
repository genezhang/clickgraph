// Q5. New groups (adapted)
// Simplified: removes collect(otherPerson)/IN pattern and OPTIONAL MATCH
// Uses direct join pattern instead
MATCH (person:Person {id: $personId})-[:KNOWS*1..2]-(otherPerson:Person)
WHERE person.id <> otherPerson.id
WITH DISTINCT otherPerson
MATCH (forum:Forum)-[:CONTAINER_OF]->(post:Post)-[:HAS_CREATOR]->(otherPerson)
RETURN forum.id AS forumId, forum.title AS forumName, count(post) AS postCount
ORDER BY postCount DESC, forumId ASC
LIMIT 20
