#!/usr/bin/env python3
"""
Test script to generate SQL for exact hop count queries using chained JOINs
"""

# Example: 2-hop query for friendships
query_2_hops = """
SELECT 
    s.user_id as start_id,
    e.user_id as end_id,
    s.full_name as start_full_name,
    e.full_name as end_full_name
FROM social.users s
JOIN social.friendships r1 ON s.user_id = r1.user1_id
JOIN social.users m1 ON r1.user2_id = m1.user_id
JOIN social.friendships r2 ON m1.user_id = r2.user1_id
JOIN social.users e ON r2.user2_id = e.user_id
WHERE s.user_id != e.user_id
  AND s.user_id != m1.user_id
  AND e.user_id != m1.user_id
"""

print("=== 2-Hop Chained JOIN Query ===")
print(query_2_hops)
print()

# Example: 3-hop query
query_3_hops = """
SELECT 
    s.user_id as start_id,
    e.user_id as end_id,
    s.full_name as start_full_name,
    e.full_name as end_full_name
FROM social.users s
JOIN social.friendships r1 ON s.user_id = r1.user1_id
JOIN social.users m1 ON r1.user2_id = m1.user_id
JOIN social.friendships r2 ON m1.user_id = r2.user1_id
JOIN social.users m2 ON r2.user2_id = m2.user_id
JOIN social.friendships r3 ON m2.user_id = r3.user1_id
JOIN social.users e ON r3.user2_id = e.user_id
WHERE s.user_id != e.user_id
  AND s.user_id != m1.user_id AND s.user_id != m2.user_id
  AND e.user_id != m1.user_id AND e.user_id != m2.user_id
  AND m1.user_id != m2.user_id
"""

print("=== 3-Hop Chained JOIN Query ===")
print(query_3_hops)
print()

print("✅ Chained JOINs Implementation:")
print("- *2 (exact 2 hops) → Uses chained JOINs (FASTER)")
print("- *3 (exact 3 hops) → Uses chained JOINs (FASTER)")
print("- *1..3 (range) → Uses recursive CTE (FLEXIBLE)")
print("- * (unbounded) → Uses recursive CTE (NECESSARY)")
