-- Setup test data for property expression validation
-- Tests: concat, dateDiff, type conversions, CASE WHEN, multiIf, JSON, arrays

USE brahmand;

-- Drop existing tables
DROP TABLE IF EXISTS users_expressions_test;
DROP TABLE IF EXISTS follows_expressions_test;

-- Create users table with various data types for expression testing
CREATE TABLE users_expressions_test (
    user_id UInt32,
    first_name String,
    last_name String,
    city String,
    
    -- Date fields (for dateDiff tests)
    registration_date Date,
    
    -- String fields to convert (for type conversion tests)
    birth_date_str String,  -- e.g., '1990-01-15'
    age_str String,         -- e.g., '34'
    score_str String,       -- e.g., '789.5'
    
    -- Numeric fields for scoring
    score UInt32,
    
    -- Boolean/status fields
    is_deleted UInt8,
    is_banned UInt8,
    is_active UInt8,
    is_premium UInt8,
    
    -- JSON field
    metadata_json String,
    
    -- CSV field (for array operations)
    tags_str String,
    
    -- Computed field (age)
    age UInt8
) ENGINE = MergeTree() ORDER BY user_id;

-- Create follows relationship table
CREATE TABLE follows_expressions_test (
    follow_id UInt64,
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date,
    interaction_count UInt32
) ENGINE = MergeTree() ORDER BY follow_id;

-- Insert test users with diverse data
INSERT INTO users_expressions_test VALUES
    (1, 'Alice', 'Smith', 'New York', '2020-01-15', '1990-05-20', '34', '1250.5', 1250, 0, 0, 1, 1, '{"subscription_type":"premium","plan":"annual"}', 'tech,music,travel', 34),
    (2, 'Bob', 'Jones', 'London', '2019-06-01', '1985-08-10', '39', '1500.0', 1500, 0, 0, 1, 1, '{"subscription_type":"premium","plan":"monthly"}', 'sports,gaming', 39),
    (3, 'Carol', 'White', 'Paris', '2021-03-10', '1995-12-05', '29', '750.25', 750, 0, 0, 1, 0, '{"subscription_type":"basic"}', 'art,food', 29),
    (4, 'David', 'Brown', 'Tokyo', '2022-11-20', '1988-04-18', '36', '600.0', 600, 0, 0, 1, 0, '{"subscription_type":"basic"}', 'anime,tech', 36),
    (5, 'Eve', 'Davis', 'Sydney', '2023-10-05', '2000-01-30', '24', '250.0', 250, 0, 0, 1, 0, '{}', 'fitness', 24),
    (6, 'Frank', 'Wilson', 'Berlin', '2024-01-01', '2005-07-14', '19', '100.0', 100, 0, 0, 1, 0, '{}', 'gaming,music,sports', 19),
    (7, 'Grace', 'Miller', 'Toronto', '2018-05-15', '1982-11-25', '42', '850.0', 850, 1, 0, 0, 0, '{"subscription_type":"cancelled"}', 'business', 42),
    (8, 'Henry', 'Taylor', 'Madrid', '2020-08-20', '1992-03-08', '32', '400.0', 400, 0, 1, 0, 0, '{"subscription_type":"suspended"}', 'travel,food', 32),
    (9, 'Iris', 'Anderson', 'Singapore', today() - INTERVAL 15 DAY, '1998-06-12', '26', '500.0', 500, 0, 0, 1, 0, '{"subscription_type":"trial"}', 'tech', 26),
    (10, 'Jack', 'Thomas', 'Dubai', today() - INTERVAL 5 DAY, '2002-09-03', '22', '50.0', 50, 0, 0, 1, 0, '{}', 'crypto,finance', 22),
    (11, 'Karen', 'Moore', 'Chicago', '2023-01-10', '2010-01-01', '14', '200.0', 200, 0, 0, 1, 0, '{}', 'games', 14),
    (12, 'Larry', 'Martin', 'Boston', '2019-12-15', '1958-04-20', '66', '900.0', 900, 0, 0, 1, 1, '{"subscription_type":"senior"}', 'news,history', 66);

-- Insert follow relationships with expressions
INSERT INTO follows_expressions_test VALUES
    (1, 1, 2, today() - INTERVAL 3 DAY, 150),
    (2, 2, 3, today() - INTERVAL 5 DAY, 80),
    (3, 3, 4, today() - INTERVAL 6 DAY, 5),
    (4, 1, 3, '2024-01-15', 250),
    (5, 2, 4, '2023-06-20', 45),
    (6, 4, 5, '2023-12-10', 2),
    (7, 5, 6, '2022-05-05', 120),
    (8, 6, 1, '2024-10-01', 8),
    (9, 9, 10, today() - INTERVAL 1 DAY, 15),
    (10, 10, 1, today() - INTERVAL 2 DAY, 100);

-- Verify data
SELECT 'Users count:' as info, count() as value FROM users_expressions_test
UNION ALL
SELECT 'Follows count:', count() FROM follows_expressions_test;

-- Show sample data
SELECT 
    user_id, 
    concat(first_name, ' ', last_name) as full_name,
    score,
    registration_date
FROM users_expressions_test 
ORDER BY user_id 
LIMIT 5;
