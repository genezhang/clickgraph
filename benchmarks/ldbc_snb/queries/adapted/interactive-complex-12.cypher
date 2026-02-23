// Q12. Expert search (adapted)
// Original uses [:HAS_TYPE|IS_SUBCLASS_OF*0..] multi-type VLP.
// Adapted to manually unroll TagClass hierarchy (4 levels) and inline into main MATCH.
// Uses directed KNOWS to avoid UNION ALL. Single WITH barrier for post-filter.
/*
:params { personId: 10995116278009, tagClassName: "Monarch" }
*/
MATCH (p:Person {id: $personId})-[:KNOWS]->(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tc0:TagClass)
OPTIONAL MATCH (tc0)-[:IS_SUBCLASS_OF]->(tc1:TagClass)
OPTIONAL MATCH (tc1)-[:IS_SUBCLASS_OF]->(tc2:TagClass)
OPTIONAL MATCH (tc2)-[:IS_SUBCLASS_OF]->(tc3:TagClass)
WITH friend, comment, tag, tc0, tc1, tc2, tc3
WHERE tc0.name = $tagClassName OR tc1.name = $tagClassName OR tc2.name = $tagClassName OR tc3.name = $tagClassName
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
ORDER BY
    replyCount DESC,
    toInteger(personId) ASC
LIMIT 20
