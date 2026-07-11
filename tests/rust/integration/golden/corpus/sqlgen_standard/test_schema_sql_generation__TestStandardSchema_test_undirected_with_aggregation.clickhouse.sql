WITH with_a_friends_cte_0 AS (SELECT anyLast(`a.full_name`) AS "p1_a_name", count(`b.user_id`) AS "friends" FROM (
SELECT 
      a.full_name AS "a.full_name",
      a.user_id AS "a.user_id",
      b.user_id AS "b.user_id"
FROM db_standard.users AS a
INNER JOIN db_standard.friendships AS t0 ON t0.user_id_1 = a.user_id
INNER JOIN db_standard.users AS b ON b.user_id = t0.user_id_2
UNION ALL 
SELECT 
      a.full_name AS "a.full_name",
      a.user_id AS "a.user_id",
      b.user_id AS "b.user_id"
FROM db_standard.users AS b
INNER JOIN db_standard.friendships AS t0 ON t0.user_id_1 = b.user_id
INNER JOIN db_standard.users AS a ON a.user_id = t0.user_id_2
) AS __union
GROUP BY `a.user_id`
)
SELECT 
      a_friends.p1_a_name AS "a.name", 
      a_friends.friends AS "friends"
FROM with_a_friends_cte_0 AS a_friends
ORDER BY a_friends.friends DESC
