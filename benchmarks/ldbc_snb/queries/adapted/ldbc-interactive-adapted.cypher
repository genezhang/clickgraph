// =============================================================================
// LDBC SNB Interactive v1 - Official Benchmark Queries
// Source: https://github.com/ldbc/ldbc_snb_interactive_v1_impls/tree/main/cypher/queries
// =============================================================================

// =============================================================================
// INTERACTIVE SHORT QUERIES (IS1-IS7) - Fast lookups
// =============================================================================

// IS1. Profile of a person
// Given a person's ID, return their profile
MATCH (n:Person {id: $personId})-[:IS_LOCATED_IN]->(p:City)
RETURN
    n.firstName AS firstName,
    n.lastName AS lastName,
    n.birthday AS birthday,
    n.locationIP AS locationIP,
    n.browserUsed AS browserUsed,
    p.id AS cityId,
    n.gender AS gender,
    n.creationDate AS creationDate

// =============================================================================
// INTERACTIVE COMPLEX QUERIES (IC1-IC14) - Challenging analytics
// =============================================================================

// IC1. Transitive friends with certain name
// Find friends up to 3 hops away with a given first name
// Uses: shortestPath, variable-length paths, complex sorting
MATCH (p:Person {id: $personId}), (friend:Person {firstName: $firstName})
WHERE NOT p=friend
WITH p, friend
MATCH path = shortestPath((p)-[:KNOWS*1..3]-(friend))
WITH min(length(path)) AS distance, friend
ORDER BY distance ASC, friend.lastName ASC, toInteger(friend.id) ASC
LIMIT 20
MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City)
OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[:IS_LOCATED_IN]->(uniCity:City)
WITH friend, collect(
    CASE uni.name WHEN null THEN null ELSE [uni.name, studyAt.classYear, uniCity.name] END
) AS unis, friendCity, distance
OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(companyCountry:Country)
WITH friend, collect(
    CASE company.name WHEN null THEN null ELSE [company.name, workAt.workFrom, companyCountry.name] END
) AS comps, unis, friendCity, distance
RETURN
    friend.id AS friendId,
    friend.lastName AS friendLastName,
    distance AS distanceFromPerson,
    friend.birthday AS friendBirthday,
    friend.creationDate AS friendCreationDate,
    friend.gender AS friendGender,
    friend.browserUsed AS friendBrowserUsed,
    friend.locationIP AS friendLocationIp,
    friend.email AS friendEmails,
    friend.language AS friendLanguages,
    friendCity.name AS friendCityName,
    unis AS friendUniversities,
    comps AS friendCompanies
ORDER BY distanceFromPerson ASC, friendLastName ASC, toInteger(friendId) ASC
LIMIT 20

// IC4. New topics
// Tags that friends posted about in a time window, but not before
MATCH (person:Person {id: $personId})-[:KNOWS]-(friend:Person),
      (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag)
WITH DISTINCT tag, post
WITH tag,
     CASE WHEN $startDate <= post.creationDate < $endDate THEN 1 ELSE 0 END AS valid,
     CASE WHEN post.creationDate < $startDate THEN 1 ELSE 0 END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount > 0 AND inValidPostCount = 0
RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10

// IC11. Job referral
// Friends within 2 hops who work at a company in a specific country, before a given year
MATCH (person:Person {id: $personId})-[:KNOWS*1..2]-(friend:Person)
WHERE NOT person = friend
WITH DISTINCT friend
MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(:Country {name: $countryName})
WHERE workAt.workFrom < $workFromYear
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    company.name AS organizationName,
    workAt.workFrom AS organizationWorkFromYear
ORDER BY
    organizationWorkFromYear ASC,
    toInteger(personId) ASC,
    organizationName DESC
LIMIT 10

// IC13. Single shortest path
// Find the shortest KNOWS path between two people
MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id}),
      path = shortestPath((person1)-[:KNOWS*]-(person2))
RETURN
    CASE path IS NULL
        WHEN true THEN -1
        ELSE length(path)
    END AS shortestPathLength

// =============================================================================
// QUERIES THAT WOULD SHOWCASE CLICKGRAPH STRENGTHS
// (ClickHouse excels at aggregations and analytical queries)
// =============================================================================

// Aggregation-heavy: Count posts per tag across all users
MATCH (post:Post)-[:HAS_TAG]->(tag:Tag)
RETURN tag.name AS tagName, count(post) AS postCount
ORDER BY postCount DESC
LIMIT 20

// Multi-hop with aggregation: Friends-of-friends activity
MATCH (p:Person {id: $personId})-[:KNOWS*1..2]->(fof:Person)<-[:HAS_CREATOR]-(post:Post)
WHERE NOT p = fof
RETURN fof.firstName, fof.lastName, count(post) AS posts
ORDER BY posts DESC
LIMIT 10

// Complex join: People who like posts by friends
MATCH (person:Person)-[:KNOWS]->(friend:Person)<-[:HAS_CREATOR]-(post:Post)<-[:LIKES]-(liker:Person)
WHERE person.id = $personId
RETURN DISTINCT liker.firstName, liker.lastName, count(post) AS likedPosts
ORDER BY likedPosts DESC
LIMIT 10

// Variable-length with filter: All people reachable within N hops
MATCH (p:Person {id: $personId})-[:KNOWS*1..4]->(reachable:Person)
RETURN count(DISTINCT reachable) AS reachableCount

// Geographic aggregation: Users per country
MATCH (p:Person)-[:IS_LOCATED_IN]->(city:City)-[:IS_PART_OF]->(country:Country)
RETURN country.name AS country, count(p) AS users
ORDER BY users DESC
LIMIT 10
