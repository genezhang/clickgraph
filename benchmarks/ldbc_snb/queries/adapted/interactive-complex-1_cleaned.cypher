

























MATCH (p:Person {id: $personId})-[:KNOWS*1..3]-(friend:Person)
WHERE friend.firstName = $firstName AND friend.id <> $personId
WITH friend, count(*) AS cnt
RETURN 
    friend.id AS friendId,
    friend.firstName AS friendFirstName,
    friend.lastName AS friendLastName
ORDER BY friend.lastName ASC, friend.id ASC
LIMIT 20
