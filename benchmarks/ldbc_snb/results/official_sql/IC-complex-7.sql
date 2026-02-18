-- LDBC Official Query: IC-complex-7
-- Status: PASS
-- Generated: 2026-02-17T19:11:55.743816
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (person:Person {id: $personId})<-[:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
--     WITH liker, message, like.creationDate AS likeTime, person
--     ORDER BY likeTime DESC, toInteger(message.id) ASC
--     WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person
-- RETURN
--     liker.id AS personId,
--     liker.firstName AS personFirstName,
--     liker.lastName AS personLastName,
--     latestLike.likeTime AS likeCreationDate,
--     latestLike.msg.id AS commentOrPostId,
--     coalesce(latestLike.msg.content, latestLike.msg.imageFile) AS commentOrPostContent,
--     toInteger(floor(toFloat(latestLike.likeTime - latestLike.msg.creationDate)/1000.0)/60.0) AS minutesLatency,
--     not((liker)-[:KNOWS]-(person)) AS isNew
-- ORDER BY
--     likeCreationDate DESC,
--     toInteger(personId) ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH with_likeTime_liker_message_person_cte_1 AS (SELECT 
      liker.firstName AS "p5_liker_firstName", 
      liker.id AS "p5_liker_id", 
      liker.lastName AS "p5_liker_lastName", 
      message.browserUsed AS "p7_message_browserUsed", 
      message.content AS "p7_message_content", 
      message.creationDate AS "p7_message_creationDate", 
      message.id AS "p7_message_id", 
      message.imageFile AS "p7_message_imageFile", 
      message.language AS "p7_message_language", 
      message.length AS "p7_message_length", 
      message.locationIP AS "p7_message_locationIP", 
      message.type AS "p7_message_type", 
      like.creationDate AS "likeTime", 
      person.birthday AS "p6_person_birthday", 
      person.browserUsed AS "p6_person_browserUsed", 
      person.creationDate AS "p6_person_creationDate", 
      person.firstName AS "p6_person_firstName", 
      person.gender AS "p6_person_gender", 
      person.id AS "p6_person_id", 
      person.lastName AS "p6_person_lastName", 
      person.locationIP AS "p6_person_locationIP"
FROM ldbc.Person AS liker
INNER JOIN ldbc.Message_hasCreator_Person AS t113 ON t113.MessageId = message.id
INNER JOIN ldbc.Person AS person ON person.id = t113.PersonId
INNER JOIN ldbc.Person_likes_Message AS like ON like.PersonId = liker.id
INNER JOIN ldbc.Message AS message ON message.id = like.MessageId
WHERE person.id = $personId
ORDER BY likeTime DESC, toInt64(message.id) ASC
), 
with_latestLike_liker_person_cte_1 AS (SELECT 
      likeTime_liker_message_person.p5_liker_firstName AS "p5_liker_firstName", 
      likeTime_liker_message_person.p5_liker_id AS "p5_liker_id", 
      likeTime_liker_message_person.p5_liker_lastName AS "p5_liker_lastName", 
      arrayElement(groupArray(map('msg', toString(message), 'likeTime', toString(likeTime))), 1) AS "latestLike", 
      likeTime_liker_message_person.p6_person_birthday AS "p6_person_birthday", 
      likeTime_liker_message_person.p6_person_browserUsed AS "p6_person_browserUsed", 
      likeTime_liker_message_person.p6_person_creationDate AS "p6_person_creationDate", 
      likeTime_liker_message_person.p6_person_firstName AS "p6_person_firstName", 
      likeTime_liker_message_person.p6_person_gender AS "p6_person_gender", 
      likeTime_liker_message_person.p6_person_id AS "p6_person_id", 
      likeTime_liker_message_person.p6_person_lastName AS "p6_person_lastName", 
      likeTime_liker_message_person.p6_person_locationIP AS "p6_person_locationIP"
FROM with_likeTime_liker_message_person_cte_1 AS likeTime_liker_message_person
)
SELECT 
      latestLike_liker_person.p5_liker_id AS "personId", 
      latestLike_liker_person.p5_liker_firstName AS "personFirstName", 
      latestLike_liker_person.p5_liker_lastName AS "personLastName", 
      latestLike_liker_person.likeTime AS "likeCreationDate", 
      latestLike_liker_person.msg AS "commentOrPostId", 
      coalesce(latestLike_liker_person.msg, latestLike_liker_person.msg) AS "commentOrPostContent", 
      toInt64(floor(toFloat64(latestLike_liker_person.likeTime - latestLike_liker_person.msg) / 1000) / 60) AS "minutesLatency", 
      NOT EXISTS (SELECT 1 FROM ldbc.Person_knows_Person WHERE (Person_knows_Person.Person1Id = liker.id AND Person_knows_Person.Person2Id = person.id) OR (Person_knows_Person.Person1Id = person.id AND Person_knows_Person.Person2Id = liker.id)) AS "isNew"
FROM with_latestLike_liker_person_cte_1 AS latestLike_liker_person
ORDER BY likeCreationDate DESC, toInt64(personId) ASC
LIMIT  20
