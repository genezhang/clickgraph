// Q7. Recent likers (adapted)
// Simplified: removes map literal {msg: message, likeTime: likeTime},
// head(collect(...)), and pattern expression not((liker)-[:KNOWS]-(person))
// Uses simple aggregation instead
MATCH (person:Person {id: $personId})<-[:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    max(like.creationDate) AS likeCreationDate
ORDER BY
    likeCreationDate DESC,
    personId ASC
LIMIT 20
