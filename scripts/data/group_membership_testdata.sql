-- Test data for group_membership.yaml schema
-- Creates groups, users, and memberships tables with sample data

-- Groups table
CREATE TABLE IF NOT EXISTS brahmand.groups (
    group_id UInt32,
    name String,
    description String
) ENGINE = Memory;

INSERT INTO brahmand.groups VALUES
    (1, 'Engineering', 'Engineering department'),
    (2, 'Backend', 'Backend team'),
    (3, 'Frontend', 'Frontend team'),
    (4, 'DevOps', 'DevOps team'),
    (5, 'Product', 'Product management'),
    (6, 'Design', 'Design team');

-- Users table
CREATE TABLE IF NOT EXISTS brahmand.users (
    user_id UInt32,
    name String,
    email String
) ENGINE = Memory;

INSERT INTO brahmand.users VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Carol', 'carol@example.com'),
    (4, 'Dave', 'dave@example.com'),
    (5, 'Eve', 'eve@example.com'),
    (6, 'Frank', 'frank@example.com');

-- Memberships table (polymorphic edge)
-- member_type is either 'User' or 'Group'
CREATE TABLE IF NOT EXISTS brahmand.memberships (
    parent_id UInt32,      -- Always refers to groups.group_id
    member_id UInt32,      -- Refers to users.user_id OR groups.group_id
    member_type String     -- 'User' or 'Group'
) ENGINE = Memory;

INSERT INTO brahmand.memberships VALUES
    -- Engineering contains Backend, Frontend, DevOps (nested groups)
    (1, 2, 'Group'),  -- Engineering -> Backend
    (1, 3, 'Group'),  -- Engineering -> Frontend
    (1, 4, 'Group'),  -- Engineering -> DevOps
    
    -- Backend team members
    (2, 1, 'User'),   -- Backend -> Alice
    (2, 2, 'User'),   -- Backend -> Bob
    
    -- Frontend team members
    (3, 3, 'User'),   -- Frontend -> Carol
    (3, 4, 'User'),   -- Frontend -> Dave
    
    -- DevOps team members
    (4, 5, 'User'),   -- DevOps -> Eve
    
    -- Product contains Design (nested group) and Frank directly
    (5, 6, 'Group'),  -- Product -> Design
    (5, 6, 'User'),   -- Product -> Frank (direct member)
    
    -- Design team members
    (6, 3, 'User');   -- Design -> Carol (shared between teams)

-- Verify data
SELECT 'Groups:' AS info;
SELECT * FROM brahmand.groups;

SELECT 'Users:' AS info;
SELECT * FROM brahmand.users;

SELECT 'Memberships:' AS info;
SELECT * FROM brahmand.memberships;

-- Test queries
SELECT 'Direct members of Engineering (should be sub-groups):' AS info;
SELECT g.name as parent, m.member_type, 
       CASE WHEN m.member_type = 'Group' THEN (SELECT name FROM brahmand.groups WHERE group_id = m.member_id)
            ELSE (SELECT name FROM brahmand.users WHERE user_id = m.member_id) END as member_name
FROM brahmand.memberships m
JOIN brahmand.groups g ON g.group_id = m.parent_id
WHERE m.parent_id = 1;

SELECT 'All User members (flat):' AS info;
SELECT g.name as group_name, u.name as user_name
FROM brahmand.memberships m
JOIN brahmand.groups g ON g.group_id = m.parent_id
JOIN brahmand.users u ON u.user_id = m.member_id AND m.member_type = 'User';
