WITH with_n_uid_cte_0 AS (SELECT 
      u.user_id AS "uid", 
      u.full_name AS "n"
FROM social.users_bench AS u
)
SELECT 
      n_uid.uid AS "uid", 
      n_uid.n AS "n"
FROM with_n_uid_cte_0 AS n_uid
ORDER BY n_uid.uid DESC NULLS FIRST, n_uid.n ASC
