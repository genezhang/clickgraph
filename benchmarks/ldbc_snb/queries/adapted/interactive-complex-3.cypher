

























MATCH (person:Person {id: $personId})-[:KNOWS*1..2]-(friend:Person)
WHERE person.id <> friend.id
WITH friend
MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:IS_LOCATED_IN]->(country:Country)
WHERE post.creationDate >= $startDate 
  AND post.creationDate < $endDate
  AND country.name IN [$countryXName, $countryYName]
RETURN 
    friend.id AS friendId,
    friend.firstName AS friendFirstName,
    friend.lastName AS friendLastName,
    count(post) AS messageCount
ORDER BY messageCount DESC, friend.id ASC
LIMIT 20
