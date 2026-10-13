WITH v1_both AS (
SELECT 
      e.since AS `since`, 
      e.user_id_1 AS `user_id_1`, 
      e.user_id_2 AS `user_id_2`, 
      e.user_id_1 AS `__cg_start_0`, 
      e.user_id_2 AS `__cg_end_0`
FROM db_standard.friendships AS e

UNION ALL
SELECT 
      e.since AS `since`, 
      e.user_id_1 AS `user_id_1`, 
      e.user_id_2 AS `user_id_2`, 
      e.user_id_2 AS `__cg_start_0`, 
      e.user_id_1 AS `__cg_end_0`
FROM db_standard.friendships AS e
WHERE e.user_id_1 <> e.user_id_2
), 
with_w2 AS (
SELECT 
      v0.user_id AS `v3__user_id`, 
      v0.full_name AS `p2_v3_name`, 
      v2.user_id AS `v4__user_id`, 
      v2.full_name AS `p2_v4_name`
FROM db_standard.users AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN db_standard.users AS v2 ON v1.__cg_end_0 = v2.user_id
)
SELECT 
      w2.p2_v3_name AS `a.name`, 
      w2.p2_v4_name AS `b.name`
FROM with_w2 AS w2

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]