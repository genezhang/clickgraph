-- Test to diagnose duplicate issue
-- Run this against your ClickHouse database

-- 1. Check if there are duplicate FOLLOWS relationships
SELECT 
    follower_id, 
    followed_id, 
    COUNT(*) as count
FROM brahmand.user_follows_bench
GROUP BY follower_id, followed_id
HAVING COUNT(*) > 1;
-- Expected: No rows (each relationship should be unique)

-- 2. Check Alice's follows
SELECT 
    u1.full_name as follower,
    u2.full_name as followed
FROM brahmand.user_follows_bench uf
JOIN brahmand.users_bench u1 ON u1.user_id = uf.follower_id
JOIN brahmand.users_bench u2 ON u2.user_id = uf.followed_id
WHERE u1.full_name = 'Alice';

-- 3. Check Bob's follows  
SELECT 
    u1.full_name as follower,
    u2.full_name as followed
FROM brahmand.user_follows_bench uf
JOIN brahmand.users_bench u1 ON u1.user_id = uf.follower_id
JOIN brahmand.users_bench u2 ON u2.user_id = uf.followed_id
WHERE u1.full_name = 'Bob';

-- 4. Check for mutual follows (this should match your expected result)
SELECT DISTINCT u_mutual.full_name as mutual
FROM brahmand.users_bench u_alice
JOIN brahmand.user_follows_bench uf_alice ON uf_alice.follower_id = u_alice.user_id
JOIN brahmand.users_bench u_mutual ON u_mutual.user_id = uf_alice.followed_id
JOIN brahmand.user_follows_bench uf_bob ON uf_bob.followed_id = u_mutual.user_id
JOIN brahmand.users_bench u_bob ON u_bob.user_id = uf_bob.follower_id
WHERE u_alice.full_name = 'Alice' 
  AND u_bob.full_name = 'Bob'
  AND u_alice.user_id != u_bob.user_id;  -- Cypher semantic: different nodes

-- 5. Same query WITHOUT DISTINCT to see if we get duplicates
SELECT u_mutual.full_name as mutual
FROM brahmand.users_bench u_alice
JOIN brahmand.user_follows_bench uf_alice ON uf_alice.follower_id = u_alice.user_id
JOIN brahmand.users_bench u_mutual ON u_mutual.user_id = uf_alice.followed_id
JOIN brahmand.user_follows_bench uf_bob ON uf_bob.followed_id = u_mutual.user_id
JOIN brahmand.users_bench u_bob ON u_bob.user_id = uf_bob.follower_id
WHERE u_alice.full_name = 'Alice' 
  AND u_bob.full_name = 'Bob'
  AND u_alice.user_id != u_bob.user_id;
