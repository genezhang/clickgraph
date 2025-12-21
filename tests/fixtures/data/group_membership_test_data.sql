-- Group Membership Test Data Fixture
-- Creates tables and loads test data for group_membership schema tests
-- Database: test_integration
-- Schema: schemas/examples/group_membership.yaml

-- Create database
CREATE DATABASE IF NOT EXISTS test_integration;

-- Create users table
CREATE TABLE IF NOT EXISTS test_integration.users (
    id UInt32,
    name String,
    email String
) ENGINE = Memory;

-- Create groups table
CREATE TABLE IF NOT EXISTS test_integration.groups (
    id UInt32,
    name String,
    description String
) ENGINE = Memory;

-- Create memberships table
CREATE TABLE IF NOT EXISTS test_integration.memberships (
    user_id UInt32,
    group_id UInt32,
    joined_at DateTime,
    role String
) ENGINE = Memory;

-- Clear existing data
TRUNCATE TABLE test_integration.users;
TRUNCATE TABLE test_integration.groups;
TRUNCATE TABLE test_integration.memberships;

-- Insert users
INSERT INTO test_integration.users VALUES
(1, 'Alice', 'alice@example.com'),
(2, 'Bob', 'bob@example.com'),
(3, 'Charlie', 'charlie@example.com'),
(4, 'Diana', 'diana@example.com'),
(5, 'Eve', 'eve@example.com'),
(6, 'Frank', 'frank@example.com'),
(7, 'Grace', 'grace@example.com'),
(8, 'Henry', 'henry@example.com');

-- Insert groups
INSERT INTO test_integration.groups VALUES
(1, 'Administrators', 'System administrators with full access'),
(2, 'Developers', 'Software development team'),
(3, 'Analysts', 'Data analysis team'),
(4, 'Support', 'Customer support team'),
(5, 'Marketing', 'Marketing and communications team');

-- Insert memberships
INSERT INTO test_integration.memberships VALUES
(1, 1, '2024-01-01 00:00:00', 'admin'),
(1, 2, '2024-01-01 00:00:00', 'member'),
(2, 2, '2024-01-02 00:00:00', 'admin'),
(3, 2, '2024-01-03 00:00:00', 'member'),
(3, 3, '2024-01-03 00:00:00', 'member'),
(4, 3, '2024-01-04 00:00:00', 'admin'),
(4, 4, '2024-01-04 00:00:00', 'member'),
(5, 4, '2024-01-05 00:00:00', 'admin'),
(6, 2, '2024-01-06 00:00:00', 'member'),
(7, 5, '2024-01-07 00:00:00', 'admin'),
(8, 5, '2024-01-08 00:00:00', 'member');

-- Verification queries (commented out, run manually if needed)
-- SELECT 'Users count:', count(*) FROM test_integration.users;
-- SELECT 'Groups count:', count(*) FROM test_integration.groups;
-- SELECT 'Memberships count:', count(*) FROM test_integration.memberships;
