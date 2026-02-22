// Q3. Popular topics in a country (adapted)
// Simplified: removes REPLY_OF*0.. VLP (multi-type polymorphic), counts posts directly
MATCH
  (:Country {name: $country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
  (person:Person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->
  (post:Post)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
RETURN
  forum.id,
  forum.title,
  forum.creationDate,
  person.id,
  count(DISTINCT post) AS messageCount
ORDER BY
  messageCount DESC,
  forum.id ASC
LIMIT 20
