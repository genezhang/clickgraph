-- IC1 Generated SQL (with issues marked)

WITH RECURSIVE with_friend_p_cte_1 AS (
  SELECT 
    p.birthday AS "p_birthday", 
    p.browserUsed AS "p_browserUsed", 
    -- ... other columns ...
    friend.locationIP AS "friend_locationIP"
  FROM ldbc.Person AS p
  INNER JOIN ldbc.Person AS friend
  WHERE p.id = 4398046511333
  LIMIT 2
), 

-- ISSUE 1: First vlp_cte1 declaration
vlp_cte1 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte1_inner AS (
      -- ... recursive CTE ...
    ),
    vlp_cte1 AS (  -- ❌ DUPLICATE DECLARATION (nested inside itself!)
      SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY end_id ORDER BY hop_count ASC) as rn
        FROM vlp_cte1_inner
      ) WHERE rn = 1
    )
    SELECT * FROM vlp_cte1
  )
),

-- ISSUE 2: rel_friend_friendCity first declaration (incorrect, should be removed)
rel_friend_friendCity AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte2_inner AS (
      -- ... recursive CTE ...
    ),
    -- ... more nested CTEs ...
  )
),

-- ISSUE 3: with_friend_cte_1 references ldbc.with_friend_p_cte_1
with_friend_cte_1 AS (
  SELECT 
    -- ... columns ...
  FROM vlp_cte1 AS vlp1
  JOIN ldbc.with_friend_p_cte_1 AS start_node  -- ❌ Should be just with_friend_p_cte_1
  -- ...
),

-- ISSUE 4: rel_friend_friendCity second declaration (the actual one)
rel_friend_friendCity AS (
  SELECT PersonId as from_node_id, CityId as to_node_id 
  FROM ldbc.Person_isLocatedIn_Place 
  UNION ALL 
  -- ...
)

-- ISSUE 5: Final SELECT references 'p' which isn't in scope
SELECT 
  friend."friend_p.p_id" AS "friend.id", 
  friendCity.name AS "friendCity.name"
FROM with_friend_cte_1 AS friend
INNER JOIN IS_LOCATED_IN::Person::Place_t2 AS t2 ON t2.PersonId = friend."friend_p.p_id"
INNER JOIN Place AS friendCity ON friendCity.id = t2.CityId
WHERE (NOT p = friend AND friendCity.type = 'City')  -- ❌ 'p' not in scope!

SETTINGS max_recursive_cte_evaluation_depth = 100
