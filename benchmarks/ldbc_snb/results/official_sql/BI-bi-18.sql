-- LDBC Official Query: BI-bi-18
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.164161
-- Database: ldbc_snb

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
FROM ldbc.Person_hasInterest_Tag AS t28
INNER JOIN ldbc.Person_knows_Person AS t29 ON t29.Person1Id = t28.PersonId
INNER JOIN ldbc.Person AS person1 ON person1.id = t29.Person1Id
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t30.Person1Id
INNER JOIN ldbc.Person_hasInterest_Tag AS t31 ON t31.PersonId = t30.Person1Id
INNER JOIN ldbc.Person_knows_Person AS t30 ON t30.Person2Id = person2.id
INNER JOIN ldbc.Person AS person2 ON person2.id = t31.PersonId
WHERE ((person1 <> person2 AND NOT EXISTS (SELECT 1 FROM ldbc.Person_knows_Person WHERE (Person_knows_Person.Person1Id = person1.id AND Person_knows_Person.Person2Id = person2.id) OR (Person_knows_Person.Person1Id = person2.id AND Person_knows_Person.Person2Id = person1.id))) AND (NOT (t28.PersonId = t28.PersonId AND t28.TagId = t28.TagId) AND NOT (t30.Person1Id = t29.Person1Id AND t30.Person2Id = t29.Person2Id)))
UNION ALL 
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id"
FROM ldbc.Person_hasInterest_Tag AS t28
INNER JOIN ldbc.Person_knows_Person AS t29 ON t29.Person1Id = t28.PersonId
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t29.Person1Id
INNER JOIN ldbc.Person_knows_Person AS t30 ON t30.Person1Id = mutualFriend.id
INNER JOIN ldbc.Person_hasInterest_Tag AS t31 ON t31.PersonId = t30.Person1Id
INNER JOIN ldbc.Tag AS tag ON tag.id = t31.TagId
INNER JOIN ldbc.Person AS person2 ON person2.id = t30.Person2Id
WHERE ((person1 <> person2 AND NOT EXISTS (SELECT 1 FROM ldbc.Person_knows_Person WHERE (Person_knows_Person.Person1Id = person1.id AND Person_knows_Person.Person2Id = person2.id) OR (Person_knows_Person.Person1Id = person2.id AND Person_knows_Person.Person2Id = person1.id))) AND NOT (t30.Person1Id = t29.Person1Id AND t30.Person2Id = t29.Person2Id))
UNION ALL 
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id"
FROM ldbc.Person_hasInterest_Tag AS t28
INNER JOIN ldbc.Person_knows_Person AS t29 ON t29.Person1Id = t28.PersonId
INNER JOIN ldbc.Person AS person1 ON person1.id = t29.Person1Id
INNER JOIN ldbc.Person_knows_Person AS t30 ON t30.Person2Id = mutualFriend.id
INNER JOIN ldbc.Person AS person2 ON person2.id = t30.Person1Id
INNER JOIN ldbc.Tag AS tag ON tag.id = t31.TagId
INNER JOIN ldbc.Person_hasInterest_Tag AS t31 ON t31.PersonId = t30.Person1Id
WHERE (person1 <> person2 AND NOT EXISTS (SELECT 1 FROM ldbc.Person_knows_Person WHERE (Person_knows_Person.Person1Id = person1.id AND Person_knows_Person.Person2Id = person2.id) OR (Person_knows_Person.Person1Id = person2.id AND Person_knows_Person.Person2Id = person1.id)))
UNION ALL 
SELECT 
      person1.id AS "person1Id", 
      person2.id AS "person2Id"
FROM ldbc.Person_hasInterest_Tag AS t28
INNER JOIN ldbc.Person_knows_Person AS t29 ON t29.Person1Id = t28.PersonId
INNER JOIN ldbc.Person AS mutualFriend ON mutualFriend.id = t29.Person1Id
INNER JOIN ldbc.Person_knows_Person AS t30 ON t30.Person2Id = mutualFriend.id
INNER JOIN ldbc.Person_hasInterest_Tag AS t31 ON t31.PersonId = t30.Person1Id
INNER JOIN ldbc.Tag AS tag ON tag.id = t31.TagId
INNER JOIN ldbc.Person AS person2 ON person2.id = t30.Person1Id
WHERE (person1 <> person2 AND NOT EXISTS (SELECT 1 FROM ldbc.Person_knows_Person WHERE (Person_knows_Person.Person1Id = person1.id AND Person_knows_Person.Person2Id = person2.id) OR (Person_knows_Person.Person1Id = person2.id AND Person_knows_Person.Person2Id = person1.id)))
) AS __union
GROUP BY "person1Id", "person2Id"
ORDER BY "mutualFriendCount" DESC, "person1Id" ASC, "person2Id" ASC
LIMIT  20
