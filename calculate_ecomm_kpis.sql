WITH

  extract_raw_data AS (

  SELECT

    PARSE_DATE('%Y%m%d', event_date) AS event_date,

    COUNT(DISTINCT(user_pseudo_id)) AS unique_users,

  

    -- Count distinct / unique combinations of user_pseudo_id and ga_session_id for number of sessions per day

    COUNT(DISTINCT(CONCAT(user_pseudo_id, '_', (

          SELECT

            value.int_value

          FROM

            UNNEST(event_params)

          WHERE

            key = 'ga_session_id')))) AS unique_sessions,

      

    -- Dataset is polluted, alternative could be to count events with event_name 'purchase'

    COUNT(DISTINCT(ecommerce.transaction_id)) AS unique_transactions,

    COALESCE(SUM(ecommerce.purchase_revenue), 0) AS ecommerce_revenue,

  FROM

    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`

  WHERE

    _TABLE_SUFFIX BETWEEN '20210128'

    AND '20210131'

  GROUP BY

    1

  ORDER BY

    1),

  calculate_rolling_sums AS (

  SELECT

    event_date,

    COUNT(DISTINCT(event_date)) OVER() AS days_in_dataset,

    unique_users,

  

    -- sessions

    unique_sessions,

    SUM(unique_sessions) OVER() AS total_sessions,

    -- change the 2 for any number for changing lookback window, but keep them all equal

    AVG(unique_sessions) OVER(ORDER BY event_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg_sessions_30d,

    -- needed to calculate rolling CVR's

    SUM(unique_sessions) OVER(ORDER BY event_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_sum_sessions_30d,

  

    -- transactions

    unique_transactions,

    SUM(unique_transactions) OVER() AS total_transactions,

    AVG(unique_transactions) OVER(ORDER BY event_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg_unique_purchases_30d,

    -- needed to calculate rolling CVR's

    SUM(unique_transactions) OVER(ORDER BY event_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_sum_unique_purchases_30d,

  

    -- revenue

    ecommerce_revenue,

    SUM(ecommerce_revenue) OVER() AS total_revenue,

    AVG(ecommerce_revenue) OVER(ORDER BY event_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg_revenue_30d,

    SUM(ecommerce_revenue) OVER(ORDER BY event_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_sum_revenue_30d,

  FROM

    extract_raw_data)

SELECT

  -- date

  event_date,

  

  -- sessions

  unique_sessions,

  total_sessions / days_in_dataset AS avg_sessions_per_day,

  rolling_avg_sessions_30d,

  

  -- calculate CVR's

  unique_transactions / unique_sessions AS daily_cvr,

  total_transactions/ total_sessions AS avg_cvr,

  rolling_sum_unique_purchases_30d / rolling_sum_sessions_30d AS rolling_cvr_30d,

  

  -- transactions

  unique_transactions,

  total_transactions / days_in_dataset AS avg_transactions_per_day,

  rolling_avg_unique_purchases_30d,

  

  -- aov

  ecommerce_revenue / unique_transactions AS aov,

  total_revenue / total_transactions AS avg_aov,

  rolling_sum_revenue_30d / rolling_sum_unique_purchases_30d AS rolling_aov_30d,

  

  -- revenue

  ecommerce_revenue,

  total_revenue / days_in_dataset AS avg_revenue_per_day,

  rolling_avg_revenue_30d

FROM

  calculate_rolling_sums

ORDER BY

  calculate_rolling_sums.event_date
