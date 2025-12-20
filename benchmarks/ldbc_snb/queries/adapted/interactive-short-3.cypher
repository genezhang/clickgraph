


















MATCH (n:Person {id: $personId})-[r:KNOWS]-(friend:Person)
RETURN
    friend.id AS personId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    r.creationDate AS friendshipCreationDate
ORDER BY r.creationDate DESC, friend.id ASC
