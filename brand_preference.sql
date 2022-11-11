SELECT

  user_id,

  brand,

  SUM(score * (timedelta * (1 / timedelta_dataset))) AS brand_score,

  ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY SUM(score * (timedelta * (1 / timedelta_dataset))) DESC) AS brand_rank

FROM (

  SELECT

    user_pseudo_id AS user_id,

    item_brand AS brand,

    event_name,

    CASE

      WHEN event_name = 'view_item_list' THEN 1

      WHEN event_name = 'view_item' THEN 5

      WHEN event_name = 'add-to-cart' THEN 10

  END

    AS score,

    PARSE_DATE('%Y%m%d', event_date) AS event_date,

    MIN(PARSE_DATE('%Y%m%d', event_date)) OVER() AS min_event_date,

    EXTRACT(DAY

    FROM (PARSE_DATE('%Y%m%d', event_date) - MIN(PARSE_DATE('%Y%m%d', event_date)) OVER())) + 1 AS timedelta,

    EXTRACT(DAY

    FROM (MAX(PARSE_DATE('%Y%m%d', event_date)) OVER() - MIN(PARSE_DATE('%Y%m%d', event_date)) OVER())) AS timedelta_dataset

  FROM

    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,

    UNNEST(items)

  WHERE

    _TABLE_SUFFIX BETWEEN '20210122'

    AND '20210131'

    AND event_name IN ('view_item_list',

      'view_item',

      'add-to-cart')

    AND item_brand NOT IN ('(not set)',

      ''))

GROUP BY

  1,

  2

-- define the maximum number of brands returned per user_pseudo_id here

-- QUALIFY brand_rank < 3

ORDER BY

  1, 4 ASC
