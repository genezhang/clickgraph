-- Generated SQL (broken):
WITH with_a_cte_1 AS (
  SELECT 
    a.city AS "a_city", 
    a.country AS "a_country", 
    a.email_address AS "a_email", 
    a.is_active AS "a_is_active", 
    a.full_name AS "a_name", 
    a.registration_date AS "a_registration_date", 
    a.user_id AS "a_user_id"
  FROM brahmand.users_bench AS a
), 
with_a_b_cte_1 AS (
  SELECT 
    a.a_city AS "a_city", 
    a.a_country AS "a_country", 
    a.a_email AS "a_email", 
    a.a_is_active AS "a_is_active", 
    a.a_name AS "a_name", 
    a.a_registration_date AS "a_registration_date", 
    a.a_user_id AS "a_user_id", 
    b.city AS "b_city", 
    b.country AS "b_country", 
    b.email_address AS "b_email", 
    b.is_active AS "b_is_active", 
    b.full_name AS "b_name", 
    b.registration_date AS "b_registration_date", 
    b.user_id AS "b_user_id"
  FROM with_a_cte_1 AS a
  INNER JOIN brahmand.user_follows_bench AS t2 ON t2.follower_id = a.a_id  -- ❌ a.a_id doesn't exist! Should be a.a_user_id
  INNER JOIN brahmand.users_bench AS b ON b.user_id = t2.followed_id
  WHERE a.user_id = 1  -- ❌ a.user_id doesn't exist! Should be a.a_user_id
), 
with_a_b_c_cte_1 AS (
  SELECT 
    c.city AS "c_city", 
    c.country AS "c_country", 
    c.email_address AS "c_email", 
    c.is_active AS "c_is_active", 
    c.full_name AS "c_name", 
    c.registration_date AS "c_registration_date", 
    c.user_id AS "c_user_id"
  FROM with_a_b_cte_1 AS a_b
  INNER JOIN brahmand.user_follows_bench AS t3 ON t3.follower_id = b.id  -- ❌ b.id doesn't exist! Should be a_b.b_user_id
  INNER JOIN brahmand.users_bench AS c ON c.user_id = t3.followed_id
)
SELECT 
  a.full_name AS "a.name",   -- ❌ a, b, c not in FROM!
  b.full_name AS "b.name", 
  c.full_name AS "c.name", 
  d.full_name AS "d.name"
FROM brahmand.user_follows_bench AS t4  -- ❌ Should be FROM with_a_b_c_cte_1 (or similar)
INNER JOIN brahmand.users_bench AS d ON d.user_id = t4.followed_id
LIMIT 3;

-- What it SHOULD be:
WITH with_a_cte_1 AS (
  SELECT 
    a.city AS "a_city", 
    a.country AS "a_country", 
    a.email_address AS "a_email", 
    a.is_active AS "a_is_active", 
    a.full_name AS "a_name", 
    a.registration_date AS "a_registration_date", 
    a.user_id AS "a_user_id"
  FROM brahmand.users_bench AS a
  WHERE a.user_id = 1  -- ✅ Filter moved to first CTE
), 
with_a_b_cte_1 AS (
  SELECT 
    a.a_city AS "a_city", 
    a.a_country AS "a_country", 
    a.a_email AS "a_email", 
    a.a_is_active AS "a_is_active", 
    a.a_name AS "a_name", 
    a.a_registration_date AS "a_registration_date", 
    a.a_user_id AS "a_user_id", 
    b.city AS "b_city", 
    b.country AS "b_country", 
    b.email_address AS "b_email", 
    b.is_active AS "b_is_active", 
    b.full_name AS "b_name", 
    b.registration_date AS "b_registration_date", 
    b.user_id AS "b_user_id"
  FROM with_a_cte_1 AS a
  INNER JOIN brahmand.user_follows_bench AS t2 ON t2.follower_id = a.a_user_id  -- ✅ Fixed
  INNER JOIN brahmand.users_bench AS b ON b.user_id = t2.followed_id
), 
with_a_b_c_cte_1 AS (
  SELECT 
    a_b.a_city AS "a_city",      -- ✅ Include a columns
    a_b.a_country AS "a_country",
    a_b.a_email AS "a_email",
    a_b.a_is_active AS "a_is_active",
    a_b.a_name AS "a_name",
    a_b.a_registration_date AS "a_registration_date",
    a_b.a_user_id AS "a_user_id",
    a_b.b_city AS "b_city",      -- ✅ Include b columns
    a_b.b_country AS "b_country",
    a_b.b_email AS "b_email",
    a_b.b_is_active AS "b_is_active",
    a_b.b_name AS "b_name",
    a_b.b_registration_date AS "b_registration_date",
    a_b.b_user_id AS "b_user_id",
    c.city AS "c_city",          -- ✅ Include c columns
    c.country AS "c_country",
    c.email_address AS "c_email",
    c.is_active AS "c_is_active",
    c.full_name AS "c_name",
    c.registration_date AS "c_registration_date",
    c.user_id AS "c_user_id"
  FROM with_a_b_cte_1 AS a_b
  INNER JOIN brahmand.user_follows_bench AS t3 ON t3.follower_id = a_b.b_user_id  -- ✅ Fixed
  INNER JOIN brahmand.users_bench AS c ON c.user_id = t3.followed_id
)
SELECT 
  a_b_c.a_name AS "a.name",   -- ✅ Use CTE alias and column names
  a_b_c.b_name AS "b.name", 
  a_b_c.c_name AS "c.name", 
  d.full_name AS "d.name"
FROM with_a_b_c_cte_1 AS a_b_c  -- ✅ Use last CTE
INNER JOIN brahmand.user_follows_bench AS t4 ON t4.follower_id = a_b_c.c_user_id  -- ✅ Add join to previous CTE
INNER JOIN brahmand.users_bench AS d ON d.user_id = t4.followed_id
LIMIT 3;
