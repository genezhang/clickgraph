# LDBC Official Queries - SQL Generation Audit

**Generated**: 2026-01-09T17:20:49.230632

**Database**: ldbc_snb

**ClickGraph**: http://localhost:8080

**Note**: This audit includes ONLY official LDBC SNB queries.

## Summary

- Total Official Queries: 41
- ✓ SQL Generation Success: 17
- ✗ SQL Generation Failed: 24
- Success Rate: 41%

## Results by Category

### BI: 5/20 (25%)

### IC: 12/21 (57%)

## Detailed Results

| Query | Category | Status | Message |
|-------|----------|--------|----------|
| BI-bi-1 | BI | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| BI-bi-10 | BI | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| BI-bi-11 | BI | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| BI-bi-12 | BI | ✗ | Render: RENDER_ERROR: Invalid render plan: No FROM clause fo |
| BI-bi-13 | BI | ✗ | Render: RENDER_ERROR: Invalid render plan: Could not render  |
| BI-bi-14 | BI | ✗ | Planning: AnalyzerError: Property 'person1Id' not found on n |
| BI-bi-15 | BI | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| BI-bi-16 | BI | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| BI-bi-17 | BI | ✗ | HTTP 400: Query syntax error: Unable to parse: >(person2:Per |
| BI-bi-18 | BI | ✓ | OK (3752 chars) |
| BI-bi-19 | BI | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| BI-bi-2 | BI | ✗ | Render: RENDER_ERROR: Invalid render plan: Cannot find ID co |
| BI-bi-20 | BI | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| BI-bi-3 | BI | ✓ | OK (2432 chars) |
| BI-bi-4 | BI | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| BI-bi-5 | BI | ✗ | Render: RENDER_ERROR: Invalid render plan: Cannot find ID co |
| BI-bi-6 | BI | ✓ | OK (813 chars) |
| BI-bi-7 | BI | ✓ | OK (731 chars) |
| BI-bi-8 | BI | ✗ | HTTP 400: Query syntax error: Unable to parse: $startDate <  |
| BI-bi-9 | BI | ✓ | OK (2079 chars) |
| IC-complex-1 | IC | ✗ | Render: RENDER_ERROR: Invalid render plan: Cannot find ID co |
| IC-complex-10 | IC | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| IC-complex-11 | IC | ✓ | OK (5605 chars) |
| IC-complex-12 | IC | ✓ | OK (925 chars) |
| IC-complex-13 | IC | ✗ | Planning: AnalyzerError: Relationship type 'KNOWS::Person::P |
| IC-complex-14 | IC | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| IC-complex-2 | IC | ✓ | OK (1319 chars) |
| IC-complex-3 | IC | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| IC-complex-4 | IC | ✗ | Render: RENDER_ERROR: Invalid render plan: Cannot find ID co |
| IC-complex-5 | IC | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| IC-complex-6 | IC | ✗ | HTTP 400: Query syntax error: Unexpected tokens after query: |
| IC-complex-7 | IC | ✗ | Planning: AnalyzerError: Property 'likeTime' not found on no |
| IC-complex-8 | IC | ✓ | OK (748 chars) |
| IC-complex-9 | IC | ✓ | OK (5064 chars) |
| IC-short-1 | IC | ✓ | OK (466 chars) |
| IC-short-2 | IC | ✓ | OK (2641 chars) |
| IC-short-3 | IC | ✓ | OK (529 chars) |
| IC-short-4 | IC | ✓ | OK (162 chars) |
| IC-short-5 | IC | ✓ | OK (272 chars) |
| IC-short-6 | IC | ✓ | OK (1656 chars) |
| IC-short-7 | IC | ✓ | OK (1799 chars) |

## Failed Queries

### BI-bi-1

**Category**: BI

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: // this should be a subquery once Cypher supports it
WITH toFloat(totalMessageCountInt) AS totalMessageCount
MATCH (message:Message)
WHERE message.cr

### BI-bi-10

**Category**: BI

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: (startPerson, {
	relationshipFilter: "KNOWS",
    minLevel: 1,
    maxLevel: $minPathDistance-1
})
YIELD node
WITH startPerson, collect(DISTINCT node

### BI-bi-11

**Category**: BI

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: 
WITH DISTINCT a, b, c
MATCH (c)-[k3:KNOWS]-(a)
WHERE $startDate <= k3.creationDate AND k3.creationDate <= $endDate
WITH DISTINCT a, b, c
RETURN coun

### BI-bi-12

**Category**: BI

**Error**: Render: RENDER_ERROR: Invalid render plan: No FROM clause found. This usually indicates missing table information or incomplete query planning.

### BI-bi-13

**Category**: BI

**Error**: Render: RENDER_ERROR: Invalid render plan: Could not render any WITH clause for alias 'zombie_zombieLikeCount'

### BI-bi-14

**Category**: BI

**Error**: Planning: AnalyzerError: Property 'person1Id' not found on node 'top'

### BI-bi-15

**Category**: BI

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: ('bi15', false)
YIELD graphName

WITH count(*) AS dummy

CALL gds.graph.project.cypher(
  'bi15',
  'MATCH (p:Person) RETURN id(p) AS id',
  'MATCH (

### BI-bi-16

**Category**: BI

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: CALL {
  WITH paramTagX, paramDateX
  MATCH (person1:Person)<-[:HAS_CREATOR]-(message1:Message)-[:HAS_TAG]->(tag:Tag {name: paramTagX})
  WHERE date(

### BI-bi-17

**Category**: BI

**Error**: HTTP 400: Query syntax error: Unable to parse: >(person2:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:HAS_TAG]->(tag),
  (forum1)<-[:HAS_MEMBER]->(person3:Person)<-[:HAS_CREATOR]-(message2:Message),
  (comment)-

### BI-bi-19

**Category**: BI

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: ('bi19', {
  sourceNode: person1,
  targetNode: person2,
  relationshipWeightProperty: 'weight'
})
YIELD totalCost
WITH person1.id AS person1Id, pers

### BI-bi-2

**Category**: BI

**Error**: Render: RENDER_ERROR: Invalid render plan: Cannot find ID column for alias 'countWindow1' needed for GROUP BY aggregation. This alias may come from a CTE that hasn't been properly registered.

### BI-bi-20

**Category**: BI

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: ('bi20', {
  sourceNode: person1,
  targetNode: person2,
  relationshipWeightProperty: 'weight'
})
YIELD totalCost
WHERE person1.id <> $person2Id
WIT

### BI-bi-4

**Category**: BI

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: CALL {
  WITH topForums
  UNWIND topForums AS topForum1
  MATCH (topForum1)-[:CONTAINER_OF]->(post:Post)<-[:REPLY_OF*0..]-(message:Message)-[:HAS_CRE

### BI-bi-5

**Category**: BI

**Error**: Render: RENDER_ERROR: Invalid render plan: Cannot find ID column for alias 'likeCount' needed for GROUP BY aggregation. This alias may come from a CTE that hasn't been properly registered.

### BI-bi-8

**Category**: BI

**Error**: HTTP 400: Query syntax error: Unable to parse: $startDate < message.creationDate
           AND message.creationDate < $endDate
WITH tag, interestedPersons, interestedPersons + collect(person) AS persons
UNWIND

### IC-complex-1

**Category**: IC

**Error**: Render: RENDER_ERROR: Invalid render plan: Cannot find ID column for alias 'distance' needed for GROUP BY aggregation. This alias may come from a CTE that hasn't been properly registered.

### IC-complex-10

**Category**: IC

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: ([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount
RETURN friend.id AS personId,
       friend.firstName AS personF

### IC-complex-13

**Category**: IC

**Error**: Planning: AnalyzerError: Relationship type 'KNOWS::Person::Person' not found in view definition

### IC-complex-14

**Category**: IC

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: CALL gds.graph.project.cypher(
  apoc.create.uuidBase64(),
  'MATCH (p:Person) RETURN id(p) AS id',
  'MATCH
      (pA:Person)-[knows:KNOWS]-(pB:Pers

### IC-complex-3

**Category**: IC

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: WITH person, countryX, countryY, collect(city) AS cities
MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city)
WHERE NOT person=friend AND N

### IC-complex-4

**Category**: IC

**Error**: Render: RENDER_ERROR: Invalid render plan: Cannot find ID column for alias 'tag' needed for GROUP BY aggregation. This alias may come from a CTE that hasn't been properly registered.

### IC-complex-5

**Category**: IC

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: 
WITH
    forum,
    collect(otherPerson) AS otherPersons
OPTIONAL MATCH (otherPerson2)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
WHERE
    oth

### IC-complex-6

**Category**: IC

**Error**: HTTP 400: Query syntax error: Unexpected tokens after query: WITH
    knownTagId,
    collect(distinct friend) as friends
UNWIND friends as f
    MATCH (f)<-[:HAS_CREATOR]-(post:Post),
          (post)-[:HAS_TA

### IC-complex-7

**Category**: IC

**Error**: Planning: AnalyzerError: Property 'likeTime' not found on node 'latestLike'

## Benchmarking Recommendation

For official LDBC SNB benchmarking, use only queries marked with ✓.

These queries match the official LDBC specification and can be compared
with results from other graph databases (Neo4j, TigerGraph, etc.)
