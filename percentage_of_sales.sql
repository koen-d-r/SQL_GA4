WITH

  cte AS (

  SELECT

    item_id,

    SUM(quantity) AS quantity,

    SUM(SUM(quantity)) OVER(ORDER BY SUM(quantity) ASC ROWS BETWEEN UNBOUNDED PRECEDING

      AND CURRENT ROW) AS rolling_sum,

    ROW_NUMBER() OVER(ORDER BY SUM(quantity) ASC) AS row_num

  FROM

    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,

    UNNEST(items)

  WHERE

    _TABLE_SUFFIX BETWEEN '20210125'

    AND '20210131'

    AND event_name = 'purchase'

    AND item_id NOT IN ('(not set)')

  GROUP BY

    1

  ORDER BY

    3)

SELECT

  item_id,

  row_num,

  row_num / MAX(row_num) OVER() AS percentual_row_num,

  quantity,

  quantity / SUM(quantity) OVER() AS percentual_sales,

  rolling_sum AS rolling_sum_quantity,

  rolling_sum / MAX(rolling_sum) OVER() AS percentual_rolling_sum

FROM

  cte

ORDER BY

  3 ASC
