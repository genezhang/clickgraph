// Q10. Friend recommendation (adapted)
// Simplified: removes datetime extraction, pattern expressions, collect/size/list comprehension
// Uses *1..2 instead of *2..2, adds :Person label to avoid multi-type VLP
MATCH (person:Person {id: $personId})-[:KNOWS*1..2]-(friend:Person)
WHERE person.id <> friend.id
WITH DISTINCT friend
MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
RETURN friend.id AS personId, friend.firstName AS personFirstName,
       friend.lastName AS personLastName, count(post) AS postCount
ORDER BY postCount DESC, personId ASC
LIMIT 10
