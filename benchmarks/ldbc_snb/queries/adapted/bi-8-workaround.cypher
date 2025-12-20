// Q8. Central Person for a Tag - ADAPTED for ClickGraph
// Workaround: Use size() on patterns directly (implemented Dec 11, 2025)
/*
:params { tag: 'Che_Guevara', startDate: datetime('2011-07-20'), endDate: datetime('2011-07-25') }
*/
MATCH (tag:Tag {name: $tag})


OPTIONAL MATCH (tag)<-[:HAS_INTEREST]-(interestedPerson:Person)
WITH tag, collect(interestedPerson) AS interestedPersons


OPTIONAL MATCH (tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(messagePerson:Person)
WHERE $startDate < message.creationDate
  AND message.creationDate < $endDate
WITH tag, interestedPersons, interestedPersons + collect(messagePerson) AS persons

UNWIND persons AS person
WITH DISTINCT tag, person


WITH
  tag,
  person,
  100 * size((tag)<-[:HAS_INTEREST]-(person)) AS interestScore
  

OPTIONAL MATCH (tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person)
WHERE $startDate < message.creationDate AND message.creationDate < $endDate
WITH tag, person, interestScore, count(message) AS messageCount
WITH tag, person, interestScore + messageCount AS score


OPTIONAL MATCH (person)-[:KNOWS]-(friend)
WITH person, score, tag, friend
OPTIONAL MATCH (tag)<-[:HAS_INTEREST]-(friend)
WITH person, score, friend, count(*) AS friendInterestCount
OPTIONAL MATCH (tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(friend)
WHERE $startDate < message.creationDate AND message.creationDate < $endDate
WITH person, score, friend, 100 * friendInterestCount + count(message) AS friendScore

RETURN
  person.id,
  score,
  sum(friendScore) AS friendsScore
ORDER BY
  score + friendsScore DESC,
  person.id ASC
LIMIT 100
