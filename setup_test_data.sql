-- Drop existing tables
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS friendships;

-- Create users table with Memory engine (Windows Docker volume constraint)
CREATE TABLE users (
    user_id UInt32,
    name String,
    age UInt8,
    city String
) ENGINE = Memory;

-- Create friendships table with Memory engine
CREATE TABLE friendships (
    user1_id UInt32,
    user2_id UInt32,
    since_date Date
) ENGINE = Memory;

-- Insert test users
INSERT INTO users (user_id, name, age, city) VALUES
(1, 'Alice', 30, 'New York'),
(2, 'Bob', 25, 'San Francisco'),
(3, 'Charlie', 35, 'Seattle'),
(4, 'Diana', 28, 'Boston'),
(5, 'Eve', 32, 'Chicago');

-- Insert friendships (note: Alice has friends, Diana has no friends for OPTIONAL MATCH testing)
INSERT INTO friendships (user1_id, user2_id, since_date) VALUES
(1, 2, '2020-01-15'),
(1, 3, '2019-05-20'),
(2, 1, '2020-01-15'),
(3, 1, '2019-05-20'),
(3, 5, '2020-11-05'),
(5, 3, '2020-11-05');
