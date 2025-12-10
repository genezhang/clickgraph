-- LDBC SNB Interactive Complex Query 3 (IC3)
-- Friends and friends of friends that have been to given countries
--
-- Given a start Person, find Persons that are their friends and friends of 
-- friends (excluding start Person) that have made Posts/Comments in both of 
-- the given Countries, X and Y, within a given period.

-- Parameters:
-- $personId: ID of the start person
-- $countryXName: Name of first country
-- $countryYName: Name of second country
-- $startDate: Start of time period (epoch milliseconds)
-- $duration: Duration in days

-- Original Cypher (simplified):
-- MATCH (person:Person {id: $personId })-[:KNOWS*1..2]-(friend)
-- WHERE NOT person = friend
-- WITH DISTINCT friend
-- MATCH (friend)<-[:HAS_CREATOR]-(message:Message),
--       (message)-[:IS_LOCATED_IN]->(country:Country)
-- WHERE message.creationDate >= $startDate 
--   AND message.creationDate < $startDate + ($duration * 86400000)
--   AND (country.name = $countryXName OR country.name = $countryYName)
-- ...

-- ClickGraph version:
MATCH (person:Person {id: $personId})-[:KNOWS*1..2]-(friend:Person)
WHERE person.id <> friend.id
WITH friend
MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:POST_LOCATED_IN]->(country:Country)
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
