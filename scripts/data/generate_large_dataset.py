#!/usr/bin/env python3
"""Generate larger test dataset for social network benchmark"""

import random
from datetime import datetime, timedelta

# Generate 100 users
users = []
for i in range(6, 106):  # Start from 6 to avoid duplicates
    city_country = [
        ("New York", "USA"), ("London", "UK"), ("Tokyo", "Japan"),
        ("Paris", "France"), ("Berlin", "Germany"), ("Toronto", "Canada"),
        ("Sydney", "Australia"), ("Mumbai", "India"), ("Singapore", "Singapore"),
        ("Dubai", "UAE")
    ]
    city, country = random.choice(city_country)
    
    user = {
        'user_id': i,
        'full_name': f"User{i}",
        'email_address': f"user{i}@example.com",
        'registration_date': (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
        'is_active': random.choice([0, 1]),
        'city': city,
        'country': country
    }
    users.append(user)

# Generate many follows relationships (each user follows 10-30 others)
follows = []
follow_id = 8  # Start from 8 to avoid duplicates
for user in users:
    num_follows = random.randint(10, 30)
    followed_ids = random.sample([u['user_id'] for u in users if u['user_id'] != user['user_id']], min(num_follows, len(users)-1))
    
    for followed_id in followed_ids:
        follow = {
            'follower_id': user['user_id'],
            'followed_id': followed_id,
            'follow_date': (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        }
        follows.append(follow)
        follow_id += 1

# Generate many posts (each user creates 5-15 posts)
posts = []
post_id = 8  # Start from 8 to avoid duplicates
for user in users:
    num_posts = random.randint(5, 15)
    for j in range(num_posts):
        post = {
            'post_id': post_id,
            'user_id': user['user_id'],
            'content': f"Post {post_id} by {user['full_name']}",
            'date': (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        }
        posts.append(post)
        post_id += 1

# Generate many likes (random likes on posts)
likes = []
for post in posts[:200]:  # Limit to avoid too many
    num_likes = random.randint(0, 10)
    likers = random.sample([u['user_id'] for u in users], min(num_likes, len(users)))
    
    for user_id in likers:
        like = {
            'user_id': user_id,
            'post_id': post['post_id'],
            'like_date': (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        }
        likes.append(like)

# Generate friendships (bidirectional, fewer than follows)
friendships = []
for i in range(200):  # 200 friendship pairs
    user1, user2 = random.sample([u['user_id'] for u in users], 2)
    if user1 > user2:
        user1, user2 = user2, user1  # Ensure consistent ordering
    
    friendship = {
        'user_id_1': user1,
        'user_id_2': user2,
        'since_date': (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
    }
    if friendship not in friendships:
        friendships.append(friendship)

print(f"Generated:")
print(f"  {len(users)} users")
print(f"  {len(follows)} follows relationships")
print(f"  {len(posts)} posts")
print(f"  {len(likes)} likes")
print(f"  {len(friendships)} friendships")

# Write SQL INSERT statements
with open('/tmp/insert_large_dataset.sql', 'w') as f:
    # Users
    f.write("-- Insert users\n")
    for user in users:
        f.write(f"INSERT INTO brahmand.users_bench VALUES ({user['user_id']}, '{user['full_name']}', '{user['email_address']}', '{user['registration_date']}', {user['is_active']}, '{user['city']}', '{user['country']}');\n")
    
    # Follows
    f.write("\n-- Insert follows\n")
    for follow in follows:
        f.write(f"INSERT INTO brahmand.user_follows_bench VALUES ({follow['follower_id']}, {follow['followed_id']}, '{follow['follow_date']}');\n")
    
    # Posts
    f.write("\n-- Insert posts\n")
    for post in posts:
        f.write(f"INSERT INTO brahmand.posts_bench VALUES ({post['post_id']}, {post['user_id']}, '{post['content']}', '{post['date']}');\n")
    
    # Likes
    f.write("\n-- Insert likes\n")
    for like in likes:
        f.write(f"INSERT INTO brahmand.post_likes_bench VALUES ({like['user_id']}, {like['post_id']}, '{like['like_date']}');\n")
    
    # Friendships
    f.write("\n-- Insert friendships\n")
    for friendship in friendships:
        f.write(f"INSERT INTO brahmand.friendships VALUES ({friendship['user_id_1']}, {friendship['user_id_2']}, '{friendship['since_date']}');\n")

print(f"\nSQL file written to /tmp/insert_large_dataset.sql")
print(f"Execute with: curl 'http://localhost:8123/?user=test_user&password=test_pass&database=brahmand' --data-binary @/tmp/insert_large_dataset.sql")
