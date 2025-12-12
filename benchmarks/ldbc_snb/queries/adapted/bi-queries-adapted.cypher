// =============================================================================
// LDBC SNB Business Intelligence (BI) Queries for ClickGraph
// =============================================================================
// These analytical queries are adapted from the official LDBC SNB BI workload
// Source: https://github.com/ldbc/ldbc_snb_bi
// 
// BI queries are designed for complex analytics - perfect for ClickHouse's 
// columnar storage and parallel execution capabilities.
//
// Schema: ldbc_snb_datagen.yaml (ldbc database)
// =============================================================================

// =============================================================================
// BI-1. Posting Summary
// Analyze message distribution by year, type, and length category
// =============================================================================

// BI-1a: Message count by year
MATCH (message:Post)
WHERE message.creationDate < '2012-01-01'
RETURN 
    message.creationDate AS creationDate,
    count(*) AS messageCount
ORDER BY messageCount DESC
LIMIT 20

// BI-1b: Comment count by creation period
MATCH (comment:Comment)
RETURN count(*) AS totalComments

// =============================================================================
// BI-2. Tag Evolution
// Track how tags are used over time
// =============================================================================

// BI-2a: Posts per tag (most popular tags)
MATCH (post:Post)-[:POST_HAS_TAG]->(tag:Tag)
RETURN 
    tag.name AS tagName,
    count(post) AS postCount
ORDER BY postCount DESC
LIMIT 100

// BI-2b: Comments per tag
MATCH (comment:Comment)-[:COMMENT_HAS_TAG]->(tag:Tag)
RETURN 
    tag.name AS tagName,
    count(comment) AS commentCount
ORDER BY commentCount DESC
LIMIT 100

// =============================================================================
// BI-3. Popular Topics in a Country
// Find forums with most messages about a topic in a country
// =============================================================================

// BI-3: Top forums by message count with specific tag class
MATCH (country:Place {name: 'China'})<-[:IS_PART_OF]-(city:Place)<-[:IS_LOCATED_IN]-(person:Person)
MATCH (person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->(post:Post)
MATCH (post)-[:POST_HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass)
WHERE tagClass.name = 'MusicalArtist'
RETURN
    forum.id AS forumId,
    forum.title AS forumTitle,
    person.id AS moderatorId,
    count(DISTINCT post) AS messageCount
ORDER BY messageCount DESC, forumId
LIMIT 20

// =============================================================================
// BI-4. Top Message Creators in Forums
// Find most active posters in large forums
// =============================================================================

// BI-4a: Forum member counts
MATCH (forum:Forum)-[:HAS_MEMBER]->(person:Person)
RETURN 
    forum.id AS forumId,
    forum.title AS forumTitle,
    count(person) AS memberCount
ORDER BY memberCount DESC
LIMIT 100

// BI-4b: Top content creators
MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)
RETURN 
    person.id AS personId,
    person.firstName AS firstName,
    person.lastName AS lastName,
    count(post) AS postCount
ORDER BY postCount DESC, personId
LIMIT 100

// =============================================================================
// BI-5. Most Active Posters of a Given Topic
// Score users based on their engagement with a specific tag
// =============================================================================

// BI-5: Active users for a tag with engagement scoring
MATCH (tag:Tag {name: 'Arnold_Schwarzenegger'})<-[:POST_HAS_TAG]-(post:Post)-[:HAS_CREATOR]->(person:Person)
RETURN 
    person.id AS personId,
    person.firstName AS firstName,
    person.lastName AS lastName,
    count(post) AS postCount
ORDER BY postCount DESC, personId
LIMIT 100

// BI-5 variant with likes (if data available)
MATCH (tag:Tag)<-[:POST_HAS_TAG]-(post:Post)-[:HAS_CREATOR]->(person:Person)
OPTIONAL MATCH (post)<-[:LIKES]-(liker:Person)
WITH person, tag, count(DISTINCT post) AS postCount, count(DISTINCT liker) AS likeCount
RETURN 
    person.id AS personId,
    tag.name AS tagName,
    postCount,
    likeCount,
    postCount + 10 * likeCount AS score
ORDER BY score DESC, personId
LIMIT 100

// =============================================================================
// BI-6. Most Authoritative Users on a Topic
// Users whose content about a tag gets engagement from other engaged users
// =============================================================================

// BI-6: Authors of posts about a tag
MATCH (tag:Tag {name: 'Che_Guevara'})<-[:POST_HAS_TAG]-(post:Post)-[:HAS_CREATOR]->(person:Person)
OPTIONAL MATCH (post)<-[:LIKES]-(liker:Person)
RETURN 
    person.id AS personId,
    person.firstName AS firstName,
    count(DISTINCT liker) AS likerCount
ORDER BY likerCount DESC, personId
LIMIT 100

// =============================================================================
// BI-7. Related Topics
// Find tags that appear on comments replying to posts with a given tag
// =============================================================================

// BI-7: Related tags through comment replies
MATCH (tag:Tag {name: 'Enrique_Iglesias'})<-[:POST_HAS_TAG]-(post:Post)
MATCH (post)<-[:REPLY_OF_POST]-(comment:Comment)-[:COMMENT_HAS_TAG]->(relatedTag:Tag)
WHERE relatedTag.id <> tag.id
RETURN 
    relatedTag.name AS relatedTagName,
    count(DISTINCT comment) AS commentCount
ORDER BY commentCount DESC, relatedTagName
LIMIT 100

// =============================================================================
// BI-8. Central Person for a Tag
// Find persons most central to discussions about a tag
// =============================================================================

// BI-8a: People interested in a tag
MATCH (tag:Tag)<-[:HAS_INTEREST]-(person:Person)
RETURN 
    tag.name AS tagName,
    count(person) AS interestedCount
ORDER BY interestedCount DESC
LIMIT 50

// BI-8b: People who post about a tag
MATCH (tag:Tag)<-[:POST_HAS_TAG]-(post:Post)-[:HAS_CREATOR]->(person:Person)
RETURN 
    tag.name AS tagName,
    person.id AS personId,
    person.firstName AS firstName,
    count(post) AS postCount
ORDER BY postCount DESC
LIMIT 100

// =============================================================================
// BI-9. Top Thread Initiators
// Find users who start the most discussion threads
// =============================================================================

// BI-9: Prolific post creators with reply counts
MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)
OPTIONAL MATCH (post)<-[:REPLY_OF_POST]-(reply:Comment)
RETURN 
    person.id AS personId,
    person.firstName AS firstName,
    person.lastName AS lastName,
    count(DISTINCT post) AS threadCount,
    count(DISTINCT reply) AS replyCount
ORDER BY replyCount DESC, personId
LIMIT 100

// =============================================================================
// BI-10. Experts in Social Circle
// Find experts (prolific taggers) within social distance of a person
// =============================================================================

// BI-10a: Friends of a person
MATCH (person:Person {id: 14})-[:KNOWS]->(friend:Person)
RETURN 
    friend.id AS friendId,
    friend.firstName AS firstName,
    friend.lastName AS lastName
ORDER BY friendId

// BI-10b: Friends who post about a tag class
MATCH (person:Person {id: 14})-[:KNOWS*1..2]->(expert:Person)
MATCH (expert)<-[:HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tc:TagClass)
WHERE tc.name = 'MusicalArtist'
RETURN DISTINCT
    expert.id AS expertId,
    expert.firstName AS firstName,
    tag.name AS tagName,
    count(post) AS postCount
ORDER BY postCount DESC, expertId
LIMIT 100

// =============================================================================
// BI-11. Friend Triangles
// Count triangles (mutual friendships) in the network
// =============================================================================

// BI-11: Triangle counting (simplified - direct pattern)
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(a)
WHERE a.id < b.id AND b.id < c.id
RETURN count(*) AS triangleCount

// =============================================================================
// BI-12. Message Count Distribution
// Distribution of message counts across persons
// =============================================================================

// BI-12: Posts per person distribution
MATCH (person:Person)
OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(post:Post)
WITH person, count(post) AS postCount
RETURN 
    postCount AS messageCount,
    count(person) AS personCount
ORDER BY personCount DESC, messageCount DESC

// =============================================================================
// BI-13. Zombies in a Country
// Find inactive users (few messages over time) in a country
// =============================================================================

// BI-13: Low-activity users by country
MATCH (country:Place {name: 'France'})<-[:IS_PART_OF]-(city:Place)<-[:IS_LOCATED_IN]-(person:Person)
OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(post:Post)
WITH person, count(post) AS messageCount
WHERE messageCount < 5
RETURN 
    person.id AS personId,
    person.firstName AS firstName,
    person.lastName AS lastName,
    messageCount
ORDER BY messageCount, personId
LIMIT 100

// =============================================================================
// BI-14. International Dialog
// Find pairs of people from different countries who interact
// =============================================================================

// BI-14: Cross-country friendships
MATCH (country1:Place {name: 'Chile'})<-[:IS_PART_OF]-(city1:Place)<-[:IS_LOCATED_IN]-(person1:Person)
MATCH (country2:Place {name: 'Argentina'})<-[:IS_PART_OF]-(city2:Place)<-[:IS_LOCATED_IN]-(person2:Person)
MATCH (person1)-[:KNOWS]-(person2)
RETURN 
    person1.id AS person1Id,
    person2.id AS person2Id,
    city1.name AS city1Name,
    city2.name AS city2Name
ORDER BY person1Id, person2Id
LIMIT 100

// =============================================================================
// BI-16. Fake News Detection (simplified)
// Find people who post about both of two tags on specific dates
// =============================================================================

// BI-16: Multi-topic posters
MATCH (person:Person)<-[:HAS_CREATOR]-(post1:Post)-[:POST_HAS_TAG]->(tag1:Tag)
MATCH (person)<-[:HAS_CREATOR]-(post2:Post)-[:POST_HAS_TAG]->(tag2:Tag)
WHERE tag1.name = 'Meryl_Streep' AND tag2.name = 'Hank_Williams'
  AND post1.id <> post2.id
RETURN DISTINCT
    person.id AS personId,
    person.firstName AS firstName,
    person.lastName AS lastName,
    count(DISTINCT post1) AS tag1Posts,
    count(DISTINCT post2) AS tag2Posts
ORDER BY tag1Posts + tag2Posts DESC, personId
LIMIT 20

// =============================================================================
// BI-17. Information Propagation Analysis
// Track how information spreads through forums
// =============================================================================

// BI-17: Forum cross-posting
MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum1:Forum)
MATCH (person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF_POST]->(post2:Post)<-[:CONTAINER_OF]-(forum2:Forum)
WHERE forum1.id <> forum2.id
RETURN 
    person.id AS personId,
    forum1.title AS sourceForumTitle,
    forum2.title AS targetForumTitle,
    count(comment) AS crossPostCount
ORDER BY crossPostCount DESC
LIMIT 20

// =============================================================================
// BI-18. Friend Recommendation
// Find potential friends through mutual connections with shared interests
// =============================================================================

// BI-18: Mutual friend recommendations
MATCH (tag:Tag {name: 'Frank_Sinatra'})<-[:HAS_INTEREST]-(person1:Person)
MATCH (person1)-[:KNOWS]-(mutualFriend:Person)-[:KNOWS]-(person2:Person)
MATCH (person2)-[:HAS_INTEREST]->(tag)
WHERE person1.id <> person2.id
  AND NOT (person1)-[:KNOWS]-(person2)
RETURN 
    person1.id AS person1Id,
    person2.id AS person2Id,
    count(DISTINCT mutualFriend) AS mutualFriendCount
ORDER BY mutualFriendCount DESC, person1Id, person2Id
LIMIT 20

// =============================================================================
// AGGREGATION QUERIES - Perfect for ClickHouse
// =============================================================================

// AGG-1: Overall statistics
MATCH (p:Person) RETURN 'Person' AS type, count(*) AS cnt
UNION ALL
MATCH (p:Post) RETURN 'Post' AS type, count(*) AS cnt
UNION ALL
MATCH (c:Comment) RETURN 'Comment' AS type, count(*) AS cnt
UNION ALL
MATCH (f:Forum) RETURN 'Forum' AS type, count(*) AS cnt
UNION ALL
MATCH (t:Tag) RETURN 'Tag' AS type, count(*) AS cnt

// AGG-2: Relationship statistics
MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
RETURN 'KNOWS' AS relType, count(*) AS cnt
UNION ALL
MATCH (p:Person)-[l:LIKES]->(post:Post)
RETURN 'LIKES' AS relType, count(*) AS cnt

// AGG-3: Geographic distribution
MATCH (p:Person)-[:IS_LOCATED_IN]->(city:Place)-[:IS_PART_OF]->(country:Place)
RETURN 
    country.name AS country,
    count(p) AS personCount
ORDER BY personCount DESC
LIMIT 20

// AGG-4: Forum activity distribution
MATCH (forum:Forum)-[:CONTAINER_OF]->(post:Post)
RETURN 
    forum.title AS forumTitle,
    count(post) AS postCount
ORDER BY postCount DESC
LIMIT 20

// AGG-5: Tag class popularity
MATCH (tag:Tag)-[:HAS_TYPE]->(tc:TagClass)
MATCH (post:Post)-[:POST_HAS_TAG]->(tag)
RETURN 
    tc.name AS tagClassName,
    count(DISTINCT tag) AS tagCount,
    count(post) AS postCount
ORDER BY postCount DESC
LIMIT 20

// =============================================================================
// COMPLEX ANALYTICAL PATTERNS
// =============================================================================

// COMPLEX-1: Engagement funnel by country
MATCH (country:Place {type: 'Country'})<-[:IS_PART_OF]-(city:Place)<-[:IS_LOCATED_IN]-(person:Person)
OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(post:Post)
OPTIONAL MATCH (post)<-[:LIKES]-(liker:Person)
WITH country, person, count(DISTINCT post) AS posts, count(DISTINCT liker) AS likes
RETURN 
    country.name AS countryName,
    count(person) AS persons,
    sum(posts) AS totalPosts,
    sum(likes) AS totalLikes
ORDER BY totalPosts DESC
LIMIT 20

// COMPLEX-2: Interest overlap analysis
MATCH (person1:Person)-[:HAS_INTEREST]->(tag:Tag)<-[:HAS_INTEREST]-(person2:Person)
WHERE person1.id < person2.id
WITH person1, person2, count(tag) AS sharedInterests
WHERE sharedInterests >= 3
RETURN 
    person1.id AS person1Id,
    person2.id AS person2Id,
    sharedInterests
ORDER BY sharedInterests DESC
LIMIT 50

// COMPLEX-3: Comment thread depth (requires variable-length paths)
MATCH (post:Post)<-[:REPLY_OF_POST]-(c1:Comment)
OPTIONAL MATCH (c1)<-[:REPLY_OF_COMMENT*1..5]-(cn:Comment)
RETURN 
    post.id AS postId,
    count(DISTINCT c1) AS directReplies,
    count(DISTINCT cn) AS deepReplies
ORDER BY deepReplies DESC
LIMIT 20

// COMPLEX-4: University alumni network
MATCH (uni:Organisation)<-[:STUDY_AT]-(person:Person)
WHERE uni.type = 'University'
WITH uni, collect(person) AS alumni
WHERE size(alumni) > 1
UNWIND alumni AS p1
MATCH (p1)-[:KNOWS]-(p2:Person)
WHERE p2 IN alumni AND p1.id < p2.id
RETURN 
    uni.name AS universityName,
    count(*) AS alumniConnections
ORDER BY alumniConnections DESC
LIMIT 20

// COMPLEX-5: Company influence network
MATCH (company:Organisation)<-[:WORK_AT]-(employee:Person)
MATCH (employee)<-[:HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
RETURN 
    company.name AS companyName,
    tag.name AS topicTag,
    count(post) AS postCount
ORDER BY postCount DESC
LIMIT 50
