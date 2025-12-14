-- Setup SQL for Social Benchmark Integration Tests
-- Database: brahmand
-- Creates benchmark tables with minimal test data

-- Create Users table
CREATE TABLE IF NOT EXISTS brahmand.users_bench (
    user_id UInt32,
    full_name String,
    email_address String,
    registration_date Date,
    is_active UInt8,
    country String,
    city String
) ENGINE = Memory;

-- Create Follows relationship table
CREATE TABLE IF NOT EXISTS brahmand.user_follows_bench (
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date,
    follow_id UInt64
) ENGINE = Memory;

-- Create Posts table
CREATE TABLE IF NOT EXISTS brahmand.posts_bench (
    post_id UInt32,
    author_id UInt32,
    content String,
    created_at DateTime,
    post_date Date
) ENGINE = Memory;

-- Create Post Likes table
CREATE TABLE IF NOT EXISTS brahmand.post_likes_bench (
    user_id UInt32,
    post_id UInt32,
    like_date DateTime
) ENGINE = Memory;

-- Create Friendships table (for FRIENDS_WITH relationship)
CREATE TABLE IF NOT EXISTS brahmand.friendships (
    user1_id UInt32,
    user2_id UInt32,
    since_date Date,
    friendship_id UInt64
) ENGINE = Memory;

-- Create Zeek Logs table (for array testing)
CREATE TABLE IF NOT EXISTS brahmand.zeek_logs (
    log_id UInt32,
    timestamp DateTime,
    source_ip String,
    dest_ips Array(String),
    protocols Array(String),
    bytes_sent UInt64
) ENGINE = Memory;

-- Insert minimal test data - Users (3 users)
INSERT INTO brahmand.users_bench
SELECT 
    number AS user_id,
    concat('User ', toString(number)) AS full_name,
    concat('user', toString(number), '@example.com') AS email_address,
    toDate('2020-01-01') + toIntervalDay(number % 365) AS registration_date,
    if(number % 3 = 0, 0, 1) AS is_active,
    arrayElement(['USA', 'UK', 'Germany', 'France'], (number % 4) + 1) AS country,
    arrayElement(['NYC', 'London', 'Berlin', 'Paris'], (number % 4) + 1) AS city
FROM numbers(3);

-- Insert Follows relationships
INSERT INTO brahmand.user_follows_bench
SELECT 
    (number % 3) AS follower_id,
    ((number + 1) % 3) AS followed_id,
    toDate('2020-01-01') + toIntervalDay(number) AS follow_date,
    number AS follow_id
FROM numbers(6)
WHERE follower_id != followed_id;

-- Insert Posts
INSERT INTO brahmand.posts_bench
SELECT
    number AS post_id,
    (number % 3) AS author_id,
    concat('Post content ', toString(number)) AS content,
    toDateTime('2020-01-01 00:00:00') + toIntervalDay(number) AS created_at,
    toDate('2020-01-01') + toIntervalDay(number) AS post_date
FROM numbers(5);

-- Insert Post Likes
INSERT INTO brahmand.post_likes_bench
SELECT
    (number % 3) AS user_id,
    (number % 5) AS post_id,
    toDateTime('2020-01-01 00:00:00') + toIntervalDay(number) AS like_date
FROM numbers(10);

-- Insert Friendships
INSERT INTO brahmand.friendships
SELECT
    (number % 3) AS user1_id,
    ((number + 1) % 3) AS user2_id,
    toDate('2020-01-01') + toIntervalDay(number) AS since_date,
    number AS friendship_id
FROM numbers(4)
WHERE user1_id != user2_id;

-- Insert Zeek Logs (for array testing)
INSERT INTO brahmand.zeek_logs
SELECT
    number AS log_id,
    toDateTime('2020-01-01 00:00:00') + toIntervalHour(number) AS timestamp,
    concat('192.168.1.', toString(number % 255)) AS source_ip,
    [concat('10.0.0.', toString((number * 2) % 255)), concat('10.0.0.', toString((number * 3) % 255))] AS dest_ips,
    ['TCP', 'HTTP'] AS protocols,
    number * 1024 AS bytes_sent
FROM numbers(5);
