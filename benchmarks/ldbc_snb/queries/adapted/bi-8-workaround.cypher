// Q8. Central Person for a Tag - ADAPTED for ClickGraph
// Full version with friend scores using 2 chained WITH clauses.
/*
:params { tag: 'Che_Guevara', startDate: datetime('2011-07-20'), endDate: datetime('2011-07-25') }
*/
MATCH (tag:Tag {name: $tag})<-[:HAS_INTEREST]-(person:Person)
OPTIONAL MATCH (tag)<-[:HAS_TAG]-(pm:Message)-[:HAS_CREATOR]->(person)
WHERE $startDate < pm.creationDate AND pm.creationDate < $endDate
WITH person, 100 + count(pm) AS score
OPTIONAL MATCH (person)-[:KNOWS]->(friend:Person)-[:HAS_INTEREST]->(tag2:Tag {name: $tag})
OPTIONAL MATCH (tag2)<-[:HAS_TAG]-(fm:Message)-[:HAS_CREATOR]->(friend)
WHERE $startDate < fm.creationDate AND fm.creationDate < $endDate
WITH person, score, friend, 100 + count(fm) AS friendScore
RETURN person.id AS personId, score, sum(friendScore) AS friendsScore
ORDER BY score + friendsScore DESC, personId ASC
LIMIT 100
