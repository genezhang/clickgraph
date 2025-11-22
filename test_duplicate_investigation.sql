-- Investigation: Why are we getting duplicate results?
-- Query pattern: (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User)
-- WHERE a.name = "Alice" AND b.name = "Bob"

-- First, let's see what the data looks like
SELECT 'Users:' as info;
SELECT user_id, full_name FROM brahmand.users_bench 
WHERE full_name IN ('Alice', 'Bob', 'Charlie', 'Diana')
ORDER BY full_name;

SELECT '' as separator;
SELECT 'Alice follows:' as info;
SELECT uf.follower_id, u1.full_name as follower, uf.followed_id, u2.full_name as followed
FROM brahmand.user_follows_bench uf
JOIN brahmand.users_bench u1 ON u1.user_id = uf.follower_id
JOIN brahmand.users_bench u2 ON u2.user_id = uf.followed_id
WHERE u1.full_name = 'Alice'
ORDER BY u2.full_name;

SELECT '' as separator;
SELECT 'Bob follows:' as info;
SELECT uf.follower_id, u1.full_name as follower, uf.followed_id, u2.full_name as followed
FROM brahmand.user_follows_bench uf
JOIN brahmand.users_bench u1 ON u1.user_id = uf.follower_id
JOIN brahmand.users_bench u2 ON u2.user_id = uf.followed_id
WHERE u1.full_name = 'Bob'
ORDER BY u2.full_name;

SELECT '' as separator;
SELECT 'Mutual follows (what we expect):' as info;
-- Users that both Alice AND Bob follow
SELECT DISTINCT u2.full_name as mutual_name
FROM brahmand.user_follows_bench uf1
JOIN brahmand.users_bench u1 ON u1.user_id = uf1.follower_id
JOIN brahmand.users_bench u2 ON u2.user_id = uf1.followed_id
WHERE u1.full_name = 'Alice'
  AND u2.user_id IN (
    SELECT uf2.followed_id 
    FROM brahmand.user_follows_bench uf2
    JOIN brahmand.users_bench u3 ON u3.user_id = uf2.follower_id
    WHERE u3.full_name = 'Bob'
  )
ORDER BY u2.full_name;

SELECT '' as separator;
SELECT 'Generated query (without DISTINCT):' as info;
-- This is what the ClickGraph query should generate
SELECT mutual.full_name AS "mutual.name"
FROM brahmand.users_bench AS b
INNER JOIN brahmand.user_follows_bench AS rel1 ON rel1.follower_id = b.user_id
INNER JOIN brahmand.users_bench AS mutual ON mutual.user_id = rel1.followed_id
INNER JOIN brahmand.user_follows_bench AS rel2 ON rel2.followed_id = mutual.user_id
INNER JOIN brahmand.users_bench AS a ON a.user_id = rel2.follower_id
WHERE b.full_name = 'Bob' AND a.full_name = 'Alice'
ORDER BY mutual.full_name;

SELECT '' as separator;
SELECT 'With DISTINCT (what we want):' as info;
SELECT DISTINCT mutual.full_name AS "mutual.name"
FROM brahmand.users_bench AS b
INNER JOIN brahmand.user_follows_bench AS rel1 ON rel1.follower_id = b.user_id
INNER JOIN brahmand.users_bench AS mutual ON mutual.user_id = rel1.followed_id
INNER JOIN brahmand.user_follows_bench AS rel2 ON rel2.followed_id = mutual.user_id
INNER JOIN brahmand.users_bench AS a ON a.user_id = rel2.follower_id
WHERE b.full_name = 'Bob' AND a.full_name = 'Alice'
ORDER BY mutual.full_name;
