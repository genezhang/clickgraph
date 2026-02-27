MATCH (person:Person {id: $personId})-[:KNOWS*1..2]-(friend:Person)
WHERE person.id <> friend.id
WITH DISTINCT friend
MATCH (friend)<-[:HAS_CREATOR]-(message:Post)-[:IS_LOCATED_IN]->(country:Country)
WHERE $endDate > message.creationDate >= $startDate
  AND country.name IN [$countryXName, $countryYName]
WITH friend,
     CASE WHEN country.name = $countryXName THEN 1 ELSE 0 END AS messageX,
     CASE WHEN country.name = $countryYName THEN 1 ELSE 0 END AS messageY
WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount > 0 AND yCount > 0
RETURN friend.id AS friendId,
       friend.firstName AS friendFirstName,
       friend.lastName AS friendLastName,
       xCount, yCount,
       xCount + yCount AS xyCount
ORDER BY xyCount DESC, friendId ASC
LIMIT 20
