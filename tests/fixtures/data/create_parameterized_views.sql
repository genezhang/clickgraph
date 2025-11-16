-- Create parameterized views in ClickHouse for multi-tenancy testing
-- ClickHouse supports parameterized views using the syntax:
-- CREATE VIEW view_name AS SELECT ... WHERE column = {param:Type}
-- Called with: SELECT * FROM view_name(param = 'value')

-- Drop existing views if they exist
DROP VIEW IF EXISTS brahmand.users_by_tenant;
DROP VIEW IF EXISTS brahmand.orders_by_tenant;
DROP VIEW IF EXISTS brahmand.friendships_by_tenant;
DROP VIEW IF EXISTS brahmand.users_by_tenant_and_country;

-- Parameterized view: Users filtered by tenant_id
CREATE VIEW brahmand.users_by_tenant AS
SELECT 
    user_id,
    tenant_id,
    name,
    email,
    country,
    created_at
FROM brahmand.multi_tenant_users
WHERE tenant_id = {tenant_id:String};

-- Parameterized view: Orders filtered by tenant_id
CREATE VIEW brahmand.orders_by_tenant AS
SELECT 
    order_id,
    tenant_id,
    user_id,
    product,
    amount,
    order_date
FROM brahmand.multi_tenant_orders
WHERE tenant_id = {tenant_id:String};

-- Parameterized view: Friendships filtered by tenant_id
CREATE VIEW brahmand.friendships_by_tenant AS
SELECT 
    friendship_id,
    tenant_id,
    user_id_from,
    user_id_to,
    friendship_date
FROM brahmand.multi_tenant_friendships
WHERE tenant_id = {tenant_id:String};

-- Multi-parameter view: Users filtered by tenant_id AND country
CREATE VIEW brahmand.users_by_tenant_and_country AS
SELECT 
    user_id,
    tenant_id,
    name,
    email,
    country,
    created_at
FROM brahmand.multi_tenant_users
WHERE tenant_id = {tenant_id:String} AND country = {country:String};

-- Test the parameterized views
SELECT 'Test 1: users_by_tenant(tenant_id = acme)' as test;
SELECT user_id, name, email FROM brahmand.users_by_tenant(tenant_id = 'acme');

SELECT 'Test 2: orders_by_tenant(tenant_id = globex)' as test;
SELECT order_id, user_id, product, amount FROM brahmand.orders_by_tenant(tenant_id = 'globex');

SELECT 'Test 3: friendships_by_tenant(tenant_id = initech)' as test;
SELECT friendship_id, user_id_from, user_id_to FROM brahmand.friendships_by_tenant(tenant_id = 'initech');

SELECT 'Test 4: users_by_tenant_and_country(tenant_id = acme, country = USA)' as test;
SELECT user_id, name, country FROM brahmand.users_by_tenant_and_country(tenant_id = 'acme', country = 'USA');

SELECT 'All parameterized views created successfully!' as status;
