WITH

  cte AS (

  SELECT

    (

    SELECT

      value.string_value

    FROM

      UNNEST(event_params)

    WHERE

      key = 'page_location') AS page_location,

    SUM((

      SELECT

        value.int_value

      FROM

        UNNEST(event_params)

      WHERE

        key = 'entrances')) AS entrances,

    COUNT(DISTINCT(CONCAT(user_pseudo_id, '_', (

          SELECT

            value.int_value

          FROM

            UNNEST(event_params)

          WHERE

            key = 'ga_session_id')))) AS unique_sessions,

  FROM

    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`

  WHERE

    _TABLE_SUFFIX BETWEEN '20210128'

    AND '20210131'

    AND event_name = 'page_view'

  GROUP BY

    1)

SELECT

  cte.*,

  entrances / unique_sessions AS entrance_rate

FROM

  cte

ORDER BY

  3 DESC
