-- LDBC Query: BI-18
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.798864
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (tag:Tag {name: 'Frank_Sinatra'})<-[:HAS_INTEREST]-(person1:Person)
-- MATCH (person1)-[:KNOWS]-(mutualFriend:Person)-[:KNOWS]-(person2:Person)
-- MATCH (person2)-[:HAS_INTEREST]->(tag)
-- WHERE person1.id <> person2.id
--   AND NOT (person1)-[:KNOWS]-(person2)
-- RETURN 
--     person1.id AS person1Id,
--     person2.id AS person2Id,
--     count(DISTINCT mutualFriend) AS mutualFriendCount
-- ORDER BY mutualFriendCount DESC, person1Id, person2Id
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
INNER JOIN ldbc.Person_hasInterest_Tag AS t53 ON t53.PersonId = person1.id
INNER JOIN ldbc.Person_knows_Person AS t54 ON t54.Person1Id = person1.id
INNER JOIN ldbc.Person_knows_Person AS t54 ON t53.PersonId = t54.Person1Id
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t54.Person2Id
INNER JOIN ldbc.Tag AS tag ON tag.id = t53.TagId
INNER JOIN ldbc.Person_knows_Person AS t55 ON t55.Person1Id = mutualFriend.id
INNER JOIN ldbc.Person_hasInterest_Tag AS t56 ON t55.Person2Id = t56.PersonId
INNER JOIN ldbc.Person AS person2 ON person2.id = t55.Person2Id
INNER JOIN ldbc.Person_hasInterest_Tag AS t56 ON t56.PersonId = person2.id
WHERE (NOT (t53.PersonId = t53.PersonId AND t53.TagId = t53.TagId) AND NOT (t55.Person1Id = t54.Person1Id AND t55.Person2Id = t54.Person2Id))
UNION ALL 
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id"
FROM ldbc.Person AS person1
INNER JOIN ldbc.Person_hasInterest_Tag AS t53 ON t53.PersonId = person1.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t53.TagId
INNER JOIN ldbc.Person_knows_Person AS t54 ON t53.PersonId = t54.Person2Id
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t54.Person1Id
INNER JOIN ldbc.Person_knows_Person AS t54 ON t54.Person2Id = person1.id
INNER JOIN ldbc.Person_knows_Person AS t55 ON t55.Person1Id = mutualFriend.id
INNER JOIN ldbc.Person_hasInterest_Tag AS t56 ON t55.Person2Id = t56.PersonId
INNER JOIN ldbc.Person AS person2 ON person2.id = t55.Person2Id
INNER JOIN ldbc.Person_hasInterest_Tag AS t56 ON t56.PersonId = person2.id
WHERE NOT (t55.Person1Id = t54.Person1Id AND t55.Person2Id = t54.Person2Id)
UNION ALL 
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id"
FROM ldbc.Person AS person1
INNER JOIN ldbc.Person_hasInterest_Tag AS t53 ON t53.PersonId = person1.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t53.TagId
INNER JOIN ldbc.Person_knows_Person AS t54 ON t53.PersonId = t54.Person1Id
INNER JOIN ldbc.Person_knows_Person AS t54 ON t54.Person1Id = person1.id
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t54.Person2Id
INNER JOIN ldbc.Person_knows_Person AS t55 ON t55.Person2Id = mutualFriend.id
INNER JOIN ldbc.Person_hasInterest_Tag AS t56 ON t55.Person1Id = t56.PersonId
INNER JOIN ldbc.Person AS person2 ON person2.id = t55.Person1Id
INNER JOIN ldbc.Person_hasInterest_Tag AS t56 ON t56.PersonId = person2.id
UNION ALL 
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id"
FROM ldbc.Person AS person1
INNER JOIN ldbc.Person_hasInterest_Tag AS t53 ON t53.PersonId = person1.id
INNER JOIN ldbc.Person_knows_Person AS t54 ON t54.Person2Id = person1.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t53.TagId
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t54.Person1Id
INNER JOIN ldbc.Person_knows_Person AS t54 ON t53.PersonId = t54.Person2Id
INNER JOIN ldbc.Person_knows_Person AS t55 ON t55.Person2Id = mutualFriend.id
INNER JOIN ldbc.Person AS person2 ON person2.id = t55.Person1Id
INNER JOIN ldbc.Person_hasInterest_Tag AS t56 ON t56.PersonId = person2.id
INNER JOIN ldbc.Person_hasInterest_Tag AS t56 ON t55.Person1Id = t56.PersonId
) AS __union
GROUP BY "person1Id", "person2Id"
ORDER BY "mutualFriendCount" DESC, "person1Id" ASC, "person2Id" ASC
LIMIT  20
