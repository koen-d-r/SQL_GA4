WITH

  other_events AS (

  SELECT

    user_pseudo_id,

    PARSE_DATE('%Y%m%d', event_date) AS event_date,

    MIN(PARSE_DATE('%Y%m%d', event_date)) OVER() AS min_event_date,

    EXTRACT(DAY

    FROM (PARSE_DATE('%Y%m%d', event_date) - MIN(PARSE_DATE('%Y%m%d', event_date)) OVER())) + 1 AS timedelta,

    EXTRACT(DAY

    FROM

      MAX(PARSE_DATE('%Y%m%d', event_date)) OVER() - MIN(PARSE_DATE('%Y%m%d', event_date)) OVER()) AS timedelta_dataset,

    item_id AS item_id,

    CONCAT(user_pseudo_id, '_', item_id) AS match_key,

    event_name,

    CASE

      WHEN event_name = 'view_item_list' THEN 1

      WHEN event_name = 'view_item' THEN 5

      WHEN event_name = 'add-to-cart' THEN 10

  END

    AS score,

  FROM

    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,

    UNNEST(items)

  WHERE

    _TABLE_SUFFIX BETWEEN '20210125'

    AND '20210131'

    AND event_name IN ('view_item_list',

      'view_item',

      'add-to-cart')

    AND item_id NOT IN ('(not set)')),

  joining_tables AS (

  SELECT

    *

  FROM

    other_events

  LEFT JOIN (

    SELECT

      CONCAT(user_pseudo_id, '_', item_id) AS match_key_purchases,

    FROM

      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,

      UNNEST(items) AS items

    WHERE

      _TABLE_SUFFIX BETWEEN '20210125'

      AND '20210131'

      AND event_name = 'purchase'

      AND item_id NOT IN ('(not set)')

    GROUP BY

      1) AS purchases

  ON

    purchases.match_key_purchases = other_events.match_key)

SELECT

  user_pseudo_id,

  item_id,

  SUM(CASE

      WHEN match_key_purchases IS NULL THEN score * (timedelta * (1 / timedelta_dataset))

    ELSE

    0

  END

    ) AS product_score,

  ROW_NUMBER() OVER(PARTITION BY user_pseudo_id ORDER BY SUM(CASE WHEN match_key_purchases IS NULL THEN score * (timedelta * (1 / timedelta_dataset))

      ELSE

      0

    END

      ) DESC) AS row_num

FROM

  joining_tables

GROUP BY

  1,

  2

ORDER BY

  user_pseudo_id,

  row_num ASC

