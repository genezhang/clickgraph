// Q10. Friend recommendation - ADAPTED for ClickGraph
// Workaround: Replace pattern comprehension with OPTIONAL MATCH + count()
/*
:params { personId: 4398046511333, month: 5 }
*/
MATCH (person:Person {id: $personId})-[:KNOWS*2..2]-(friend),
       (friend)-[:IS_LOCATED_IN]->(city:City)
WHERE NOT friend=person AND
      NOT (friend)-[:KNOWS]-(person)
WITH person, city, friend, datetime({epochMillis: friend.birthday}) as birthday
WHERE  (toMonth(birthday)=$month AND toDayOfMonth(birthday)>=21) OR
        (toMonth(birthday)=($month%12)+1 AND toDayOfMonth(birthday)<22)
WITH DISTINCT friend, city, person


OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
WITH friend, city, person, collect(post) AS allPosts


OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(commonPost:Post)
WHERE (commonPost)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)
WITH friend, 
     city, 
     size(allPosts) AS postCount,
     count(commonPost) AS commonPostCount

RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10
