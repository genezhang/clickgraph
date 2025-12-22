-- LDBC Official Query: BI-bi-18
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.068840
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (tag:Tag {name: $tag})<-[:HAS_INTEREST]-(person1:Person)-[:KNOWS]-(mutualFriend:Person)-[:KNOWS]-(person2:Person)-[:HAS_INTEREST]->(tag)
-- WHERE person1 <> person2
--   AND NOT (person1)-[:KNOWS]-(person2)
-- RETURN person1.id AS person1Id, person2.id AS person2Id, count(DISTINCT mutualFriend) AS mutualFriendCount
-- ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT 
      "person1Id" AS "person1Id", 
      "person2Id" AS "person2Id", 
      count(DISTINCT mutualFriend.id) AS "mutualFriendCount"
FROM (
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id"
FROM ldbc.Person AS person1
INNER JOIN ldbc.Person_hasInterest_Tag AS t148 ON t148.PersonId = person1.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t148.TagId
INNER JOIN ldbc.Person_knows_Person AS t149 ON t149.Person1Id = person1.id
INNER JOIN ldbc.Person_knows_Person AS t149 ON t148.PersonId = t149.Person1Id
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t149.Person2Id
INNER JOIN ldbc.Person_knows_Person AS t150 ON t150.Person1Id = mutualFriend.id
INNER JOIN ldbc.Person_hasInterest_Tag AS t151 ON t150.Person2Id = t151.PersonId
INNER JOIN ldbc.Person AS person2 ON person2.id = t150.Person2Id
INNER JOIN ldbc.Person_hasInterest_Tag AS t151 ON t151.PersonId = person2.id
WHERE (NOT (t148.PersonId = t148.PersonId AND t148.TagId = t148.TagId) AND NOT (t150.Person1Id = t149.Person1Id AND t150.Person2Id = t149.Person2Id))
UNION ALL 
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id"
FROM ldbc.Person AS person1
INNER JOIN ldbc.Person_hasInterest_Tag AS t148 ON t148.PersonId = person1.id
INNER JOIN ldbc.Person_knows_Person AS t149 ON t149.Person2Id = person1.id
INNER JOIN ldbc.Person_knows_Person AS t149 ON t148.PersonId = t149.Person2Id
INNER JOIN ldbc.Tag AS tag ON tag.id = t148.TagId
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t149.Person1Id
INNER JOIN ldbc.Person_knows_Person AS t150 ON t150.Person1Id = mutualFriend.id
INNER JOIN ldbc.Person AS person2 ON person2.id = t150.Person2Id
INNER JOIN ldbc.Person_hasInterest_Tag AS t151 ON t151.PersonId = person2.id
INNER JOIN ldbc.Person_hasInterest_Tag AS t151 ON t150.Person2Id = t151.PersonId
WHERE NOT (t150.Person1Id = t149.Person1Id AND t150.Person2Id = t149.Person2Id)
UNION ALL 
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id"
FROM ldbc.Person AS person1
INNER JOIN ldbc.Person_hasInterest_Tag AS t148 ON t148.PersonId = person1.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t148.TagId
INNER JOIN ldbc.Person_knows_Person AS t149 ON t149.Person1Id = person1.id
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t149.Person2Id
INNER JOIN ldbc.Person_knows_Person AS t150 ON t150.Person2Id = mutualFriend.id
INNER JOIN ldbc.Person_knows_Person AS t149 ON t148.PersonId = t149.Person1Id
INNER JOIN ldbc.Person AS person2 ON person2.id = t150.Person1Id
INNER JOIN ldbc.Person_hasInterest_Tag AS t151 ON t150.Person1Id = t151.PersonId
INNER JOIN ldbc.Person_hasInterest_Tag AS t151 ON t151.PersonId = person2.id
UNION ALL 
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id"
FROM ldbc.Person AS person1
INNER JOIN ldbc.Person_hasInterest_Tag AS t148 ON t148.PersonId = person1.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t148.TagId
INNER JOIN ldbc.Person_knows_Person AS t149 ON t149.Person2Id = person1.id
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t149.Person1Id
INNER JOIN ldbc.Person_knows_Person AS t149 ON t148.PersonId = t149.Person2Id
INNER JOIN ldbc.Person_knows_Person AS t150 ON t150.Person2Id = mutualFriend.id
INNER JOIN ldbc.Person_hasInterest_Tag AS t151 ON t150.Person1Id = t151.PersonId
INNER JOIN ldbc.Person AS person2 ON person2.id = t150.Person1Id
INNER JOIN ldbc.Person_hasInterest_Tag AS t151 ON t151.PersonId = person2.id
) AS __union
GROUP BY "person1Id", "person2Id"
ORDER BY "mutualFriendCount" DESC, "person1Id" ASC, "person2Id" ASC
LIMIT  20
