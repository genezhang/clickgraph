#!/bin/bash
# Setup test data for social_integration schema
# Database: test_integration
# Tables: users_test, posts_test, user_follows_test, post_likes_test

set -e

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-test_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-test_pass}"
DATABASE="test_integration"

echo "🔧 Setting up social_integration test data..."
echo "  Database: $DATABASE"
echo "  Host: $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"

# Helper function to execute SQL
execute_sql() {
    local sql="$1"
    curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/" \
        --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
        --data-binary "$sql"
}

# Create database if not exists
echo "  → Creating database..."
execute_sql "CREATE DATABASE IF NOT EXISTS ${DATABASE}"

# Create tables
echo "  → Creating tables..."

# Users table
execute_sql "
CREATE TABLE IF NOT EXISTS ${DATABASE}.users_test (
    user_id UInt32,
    full_name String,
    email_address String,
    age UInt8,
    registration_date Date,
    is_active UInt8,
    country String,
    city String
) ENGINE = Memory"

# Posts table
execute_sql "
CREATE TABLE IF NOT EXISTS ${DATABASE}.posts_test (
    post_id UInt32,
    post_title String,
    post_content String,
    post_date Date,
    author_id UInt32
) ENGINE = Memory"

# User follows table
execute_sql "
CREATE TABLE IF NOT EXISTS ${DATABASE}.user_follows_test (
    follow_id UInt32,
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = Memory"

# Post likes table
execute_sql "
CREATE TABLE IF NOT EXISTS ${DATABASE}.post_likes_test (
    like_id UInt32,
    user_id UInt32,
    post_id UInt32,
    like_date Date
) ENGINE = Memory"

# Insert test data
echo "  → Inserting test data..."

# Insert 30 users
execute_sql "
INSERT INTO ${DATABASE}.users_test VALUES
    (1, 'Alice Johnson', 'alice@example.com', 28, '2020-01-15', 1, 'USA', 'New York'),
    (2, 'Bob Smith', 'bob@example.com', 32, '2019-05-20', 1, 'UK', 'London'),
    (3, 'Carol White', 'carol@example.com', 25, '2021-03-10', 1, 'Canada', 'Toronto'),
    (4, 'David Brown', 'david@example.com', 35, '2018-11-25', 1, 'Australia', 'Sydney'),
    (5, 'Eve Davis', 'eve@example.com', 29, '2020-07-08', 1, 'USA', 'San Francisco'),
    (6, 'Frank Miller', 'frank@example.com', 31, '2019-09-14', 1, 'Germany', 'Berlin'),
    (7, 'Grace Lee', 'grace@example.com', 27, '2020-12-01', 1, 'South Korea', 'Seoul'),
    (8, 'Henry Wilson', 'henry@example.com', 33, '2019-02-18', 1, 'USA', 'Chicago'),
    (9, 'Iris Martinez', 'iris@example.com', 26, '2021-06-22', 1, 'Spain', 'Madrid'),
    (10, 'Jack Taylor', 'jack@example.com', 30, '2020-04-11', 1, 'USA', 'Boston'),
    (11, 'Kate Anderson', 'kate@example.com', 28, '2020-08-19', 1, 'UK', 'Manchester'),
    (12, 'Liam Thomas', 'liam@example.com', 34, '2018-10-05', 1, 'Ireland', 'Dublin'),
    (13, 'Mia Jackson', 'mia@example.com', 24, '2021-01-30', 1, 'USA', 'Austin'),
    (14, 'Noah Harris', 'noah@example.com', 36, '2017-12-12', 1, 'Canada', 'Vancouver'),
    (15, 'Olivia Clark', 'olivia@example.com', 29, '2020-05-25', 1, 'Australia', 'Melbourne'),
    (16, 'Paul Lewis', 'paul@example.com', 31, '2019-07-14', 1, 'USA', 'Seattle'),
    (17, 'Quinn Walker', 'quinn@example.com', 27, '2020-11-08', 1, 'UK', 'Edinburgh'),
    (18, 'Rachel Hall', 'rachel@example.com', 25, '2021-02-16', 1, 'USA', 'Portland'),
    (19, 'Sam Allen', 'sam@example.com', 32, '2019-04-22', 1, 'Canada', 'Montreal'),
    (20, 'Tina Young', 'tina@example.com', 28, '2020-09-03', 1, 'USA', 'Denver'),
    (21, 'Uma King', 'uma@example.com', 26, '2021-05-17', 1, 'India', 'Mumbai'),
    (22, 'Victor Wright', 'victor@example.com', 35, '2018-08-29', 1, 'USA', 'Miami'),
    (23, 'Wendy Lopez', 'wendy@example.com', 30, '2020-03-12', 1, 'Mexico', 'Mexico City'),
    (24, 'Xavier Hill', 'xavier@example.com', 33, '2019-06-05', 1, 'USA', 'Dallas'),
    (25, 'Yara Scott', 'yara@example.com', 27, '2020-10-20', 1, 'UAE', 'Dubai'),
    (26, 'Zack Green', 'zack@example.com', 29, '2020-02-14', 1, 'USA', 'Phoenix'),
    (27, 'Amy Adams', 'amy@example.com', 31, '2019-11-30', 1, 'UK', 'Bristol'),
    (28, 'Ben Baker', 'ben@example.com', 28, '2020-07-25', 1, 'USA', 'Atlanta'),
    (29, 'Chloe Carter', 'chloe@example.com', 26, '2021-04-08', 1, 'Canada', 'Calgary'),
    (30, 'Dan Foster', 'dan@example.com', 34, '2018-09-16', 0, 'USA', 'Detroit')"

# Insert 50 posts
execute_sql "
INSERT INTO ${DATABASE}.posts_test VALUES
    (1, 'Introduction', 'Hello everyone!', '2023-01-01', 1),
    (2, 'First Post', 'My first post here', '2023-01-02', 2),
    (3, 'Tech News', 'Latest in technology', '2023-01-03', 3),
    (4, 'Travel Blog', 'My trip to Europe', '2023-01-04', 4),
    (5, 'Cooking Tips', 'Best pasta recipe', '2023-01-05', 5),
    (6, 'Music Review', 'New album review', '2023-01-06', 6),
    (7, 'Book Club', 'This month''s book', '2023-01-07', 7),
    (8, 'Fitness Journey', 'Week 1 progress', '2023-01-08', 8),
    (9, 'Photography', 'Sunset shots', '2023-01-09', 9),
    (10, 'Gaming News', 'New game release', '2023-01-10', 10),
    (11, 'Movie Review', 'Latest blockbuster', '2023-01-11', 1),
    (12, 'DIY Projects', 'Home improvement', '2023-01-12', 2),
    (13, 'Pet Stories', 'My dog''s adventure', '2023-01-13', 3),
    (14, 'Fashion Trends', 'Spring collection', '2023-01-14', 4),
    (15, 'Career Advice', 'Job hunting tips', '2023-01-15', 5),
    (16, 'Investment Tips', 'Stock market basics', '2023-01-16', 6),
    (17, 'Gardening', 'Growing tomatoes', '2023-01-17', 7),
    (18, 'Art Exhibition', 'Local art show', '2023-01-18', 8),
    (19, 'Science Facts', 'Space discoveries', '2023-01-19', 9),
    (20, 'History Lesson', 'Ancient Rome', '2023-01-20', 10),
    (21, 'Language Learning', 'Spanish basics', '2023-01-21', 11),
    (22, 'Environment', 'Climate change', '2023-01-22', 12),
    (23, 'Psychology', 'Understanding emotions', '2023-01-23', 13),
    (24, 'Philosophy', 'Existentialism 101', '2023-01-24', 14),
    (25, 'Economics', 'Supply and demand', '2023-01-25', 15),
    (26, 'Politics', 'Current events', '2023-01-26', 16),
    (27, 'Health Tips', 'Staying healthy', '2023-01-27', 17),
    (28, 'Education', 'Online learning', '2023-01-28', 18),
    (29, 'Technology', 'AI advancements', '2023-01-29', 19),
    (30, 'Social Media', 'Trends analysis', '2023-01-30', 20),
    (31, 'Sports News', 'Championship results', '2023-01-31', 21),
    (32, 'Weather', 'Weekly forecast', '2023-02-01', 22),
    (33, 'Astronomy', 'Meteor shower', '2023-02-02', 23),
    (34, 'Architecture', 'Modern designs', '2023-02-03', 24),
    (35, 'Dance', 'New choreography', '2023-02-04', 25),
    (36, 'Theater', 'Play review', '2023-02-05', 26),
    (37, 'Comedy', 'Stand-up special', '2023-02-06', 27),
    (38, 'Documentary', 'Nature series', '2023-02-07', 28),
    (39, 'Podcast', 'Episode highlights', '2023-02-08', 29),
    (40, 'Newsletter', 'Weekly roundup', '2023-02-09', 30),
    (41, 'Update', 'Life changes', '2023-02-10', 1),
    (42, 'Announcement', 'Big news!', '2023-02-11', 2),
    (43, 'Tutorial', 'How-to guide', '2023-02-12', 3),
    (44, 'Opinion', 'My thoughts on...', '2023-02-13', 4),
    (45, 'Interview', 'Q&A session', '2023-02-14', 5),
    (46, 'Case Study', 'Success story', '2023-02-15', 6),
    (47, 'Research', 'Study findings', '2023-02-16', 7),
    (48, 'Survey', 'Community poll', '2023-02-17', 8),
    (49, 'Event', 'Conference recap', '2023-02-18', 9),
    (50, 'Reflection', 'Looking back', '2023-02-19', 10)"

# Insert 60 follows relationships
execute_sql "
INSERT INTO ${DATABASE}.user_follows_test VALUES
    (1, 1, 2, '2023-01-01'), (2, 1, 3, '2023-01-02'), (3, 1, 5, '2023-01-03'),
    (4, 2, 1, '2023-01-04'), (5, 2, 4, '2023-01-05'), (6, 2, 6, '2023-01-06'),
    (7, 3, 1, '2023-01-07'), (8, 3, 7, '2023-01-08'), (9, 3, 8, '2023-01-09'),
    (10, 4, 2, '2023-01-10'), (11, 4, 9, '2023-01-11'), (12, 4, 10, '2023-01-12'),
    (13, 5, 1, '2023-01-13'), (14, 5, 11, '2023-01-14'), (15, 5, 12, '2023-01-15'),
    (16, 6, 2, '2023-01-16'), (17, 6, 13, '2023-01-17'), (18, 6, 14, '2023-01-18'),
    (19, 7, 3, '2023-01-19'), (20, 7, 15, '2023-01-20'), (21, 7, 16, '2023-01-21'),
    (22, 8, 3, '2023-01-22'), (23, 8, 17, '2023-01-23'), (24, 8, 18, '2023-01-24'),
    (25, 9, 4, '2023-01-25'), (26, 9, 19, '2023-01-26'), (27, 9, 20, '2023-01-27'),
    (28, 10, 4, '2023-01-28'), (29, 10, 21, '2023-01-29'), (30, 10, 22, '2023-01-30'),
    (31, 11, 5, '2023-02-01'), (32, 11, 23, '2023-02-02'), (33, 11, 24, '2023-02-03'),
    (34, 12, 6, '2023-02-04'), (35, 12, 25, '2023-02-05'), (36, 12, 26, '2023-02-06'),
    (37, 13, 7, '2023-02-07'), (38, 13, 27, '2023-02-08'), (39, 13, 28, '2023-02-09'),
    (40, 14, 8, '2023-02-10'), (41, 14, 29, '2023-02-11'), (42, 14, 30, '2023-02-12'),
    (43, 15, 9, '2023-02-13'), (44, 15, 1, '2023-02-14'), (45, 15, 2, '2023-02-15'),
    (46, 16, 10, '2023-02-16'), (47, 16, 3, '2023-02-17'), (48, 16, 4, '2023-02-18'),
    (49, 17, 11, '2023-02-19'), (50, 17, 5, '2023-02-20'), (51, 17, 6, '2023-02-21'),
    (52, 18, 12, '2023-02-22'), (53, 18, 7, '2023-02-23'), (54, 18, 8, '2023-02-24'),
    (55, 19, 13, '2023-02-25'), (56, 19, 9, '2023-02-26'), (57, 19, 10, '2023-02-27'),
    (58, 20, 14, '2023-02-28'), (59, 20, 11, '2023-03-01'), (60, 20, 12, '2023-03-02')"

# Insert 80 likes
execute_sql "
INSERT INTO ${DATABASE}.post_likes_test VALUES
    (1, 1, 2, '2023-01-02'), (2, 1, 3, '2023-01-03'), (3, 1, 5, '2023-01-05'),
    (4, 2, 1, '2023-01-01'), (5, 2, 3, '2023-01-03'), (6, 2, 6, '2023-01-06'),
    (7, 3, 1, '2023-01-01'), (8, 3, 7, '2023-01-07'), (9, 3, 8, '2023-01-08'),
    (10, 4, 2, '2023-01-02'), (11, 4, 9, '2023-01-09'), (12, 4, 10, '2023-01-10'),
    (13, 5, 1, '2023-01-01'), (14, 5, 11, '2023-01-11'), (15, 5, 12, '2023-01-12'),
    (16, 6, 2, '2023-01-02'), (17, 6, 13, '2023-01-13'), (18, 6, 14, '2023-01-14'),
    (19, 7, 3, '2023-01-03'), (20, 7, 15, '2023-01-15'), (21, 7, 16, '2023-01-16'),
    (22, 8, 3, '2023-01-03'), (23, 8, 17, '2023-01-17'), (24, 8, 18, '2023-01-18'),
    (25, 9, 4, '2023-01-04'), (26, 9, 19, '2023-01-19'), (27, 9, 20, '2023-01-20'),
    (28, 10, 4, '2023-01-04'), (29, 10, 21, '2023-01-21'), (30, 10, 22, '2023-01-22'),
    (31, 11, 5, '2023-01-05'), (32, 11, 23, '2023-01-23'), (33, 11, 24, '2023-01-24'),
    (34, 12, 6, '2023-01-06'), (35, 12, 25, '2023-01-25'), (36, 12, 26, '2023-01-26'),
    (37, 13, 7, '2023-01-07'), (38, 13, 27, '2023-01-27'), (39, 13, 28, '2023-01-28'),
    (40, 14, 8, '2023-01-08'), (41, 14, 29, '2023-01-29'), (42, 14, 30, '2023-01-30'),
    (43, 15, 9, '2023-01-09'), (44, 15, 1, '2023-01-01'), (45, 15, 2, '2023-01-02'),
    (46, 16, 10, '2023-01-10'), (47, 16, 3, '2023-01-03'), (48, 16, 4, '2023-01-04'),
    (49, 17, 11, '2023-01-11'), (50, 17, 5, '2023-01-05'), (51, 17, 6, '2023-01-06'),
    (52, 18, 12, '2023-01-12'), (53, 18, 7, '2023-01-07'), (54, 18, 8, '2023-01-08'),
    (55, 19, 13, '2023-01-13'), (56, 19, 9, '2023-01-09'), (57, 19, 10, '2023-01-10'),
    (58, 20, 14, '2023-01-14'), (59, 20, 11, '2023-01-11'), (60, 20, 12, '2023-01-12'),
    (61, 21, 15, '2023-01-15'), (62, 21, 13, '2023-01-13'), (63, 21, 14, '2023-01-14'),
    (64, 22, 16, '2023-01-16'), (65, 22, 15, '2023-01-15'), (66, 22, 17, '2023-01-17'),
    (67, 23, 18, '2023-01-18'), (68, 23, 19, '2023-01-19'), (69, 23, 20, '2023-01-20'),
    (70, 24, 21, '2023-01-21'), (71, 24, 22, '2023-01-22'), (72, 24, 23, '2023-01-23'),
    (73, 25, 24, '2023-01-24'), (74, 25, 25, '2023-01-25'), (75, 25, 26, '2023-01-26'),
    (76, 26, 27, '2023-01-27'), (77, 27, 28, '2023-01-28'), (78, 28, 29, '2023-01-29'),
    (79, 29, 30, '2023-01-30'), (80, 30, 1, '2023-01-01')"

echo "✅ Social integration test data loaded successfully!"
echo ""
echo "📊 Data summary:"
execute_sql "SELECT 'users' as table, count(*) as rows FROM ${DATABASE}.users_test
            UNION ALL SELECT 'posts', count(*) FROM ${DATABASE}.posts_test
            UNION ALL SELECT 'follows', count(*) FROM ${DATABASE}.user_follows_test
            UNION ALL SELECT 'likes', count(*) FROM ${DATABASE}.post_likes_test
            FORMAT PrettyCompact"
