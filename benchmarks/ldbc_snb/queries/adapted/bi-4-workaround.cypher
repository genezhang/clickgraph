// Q4. Top message creators in a country - ADAPTED for ClickGraph
// Workaround: Replace CALL subquery with UNION ALL restructuring
/*
:params { date: datetime('2010-01-29') }
*/
MATCH (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_MEMBER]-(forum:Forum)
WHERE forum.creationDate > $date
WITH country, forum, count(person) AS numberOfMembers
ORDER BY numberOfMembers DESC, forum.id ASC, country.id
WITH DISTINCT forum AS topForum
LIMIT 100
WITH collect(topForum) AS topForums


UNWIND topForums AS topForum1
MATCH (topForum1)-[:CONTAINER_OF]->(post:Post)<-[:REPLY_OF*0..]-(message:Message)-[:HAS_CREATOR]->(person:Person)<-[:HAS_MEMBER]-(topForum2:Forum)
WHERE topForum2 IN topForums
WITH person, count(DISTINCT message) AS messageCount

UNION ALL


UNWIND topForums AS topForum1
MATCH (person:Person)<-[:HAS_MEMBER]-(topForum1:Forum)
WITH person, 0 AS messageCount


WITH person, sum(messageCount) AS totalMessageCount
RETURN
  person.id AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  person.creationDate AS personCreationDate,
  totalMessageCount AS messageCount
ORDER BY
  messageCount DESC,
  person.id ASC
LIMIT 100



