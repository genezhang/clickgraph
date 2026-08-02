WITH with_one_u_cte_0 AS (SELECT 
      u.user_id AS `p1_u_user_id`, 
      1 AS `one`
FROM social.users_bench AS u
)
SELECT 
      count(one_u.one) AS `c`, 
      count(one_u.p1_u_user_id) AS `uc`
FROM with_one_u_cte_0 AS one_u
