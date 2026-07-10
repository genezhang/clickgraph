WITH with_e_n_u_cte_0 AS (SELECT 
      u.email_address AS `p1_u_email`, 
      u.full_name AS `p1_u_name`, 
      u.user_id AS `p1_u_user_id`
FROM test_integration.users_test AS u
)
SELECT 
      e_n_u.p1_u_email AS `n.email`, 
      e_n_u.p1_u_email AS `n.email_address`, 
      e_n_u.p1_u_name AS `n.full_name`, 
      e_n_u.p1_u_name AS `n.name`, 
      e_n_u.p1_u_user_id AS `n.user_id`, 
      e_n_u.p1_u_email AS `e.email`, 
      e_n_u.p1_u_email AS `e.email_address`, 
      e_n_u.p1_u_name AS `e.full_name`, 
      e_n_u.p1_u_name AS `e.name`, 
      e_n_u.p1_u_user_id AS `e.user_id`
FROM with_e_n_u_cte_0 AS e_n_u
LIMIT 1