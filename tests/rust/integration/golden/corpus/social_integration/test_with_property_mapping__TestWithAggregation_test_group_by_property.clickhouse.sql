WITH with_cnt_country_cte_0 AS (SELECT 
      u.country AS "country", 
      count(*) AS "cnt"
FROM test_integration.users_test AS u
GROUP BY u.country
)
SELECT 
      cnt_country.country AS "country", 
      cnt_country.cnt AS "cnt"
FROM with_cnt_country_cte_0 AS cnt_country
LIMIT 5