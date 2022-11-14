WITH

  cte AS (

  SELECT

    ga_session_id,

    event_timestamp,

    page_location,

    ROW_NUMBER() OVER(PARTITION BY ga_session_id ORDER BY event_timestamp DESC) AS row_num

  FROM (

    SELECT

      CONCAT(user_pseudo_id, '_', (

        SELECT

          value.int_value

        FROM

          UNNEST(event_params)

        WHERE

          key = 'ga_session_id')) AS ga_session_id,

      event_timestamp,

      (

      SELECT

        value.string_value

      FROM

        UNNEST(event_params)

      WHERE

        key = 'page_location') AS page_location,

    FROM

      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`

    WHERE

      event_name = 'page_view' AND

      _TABLE_SUFFIX BETWEEN '20210125' AND '20210131'

    ORDER BY

      1))

SELECT

  page_location,

  SUM(CASE

      WHEN row_num = 1 THEN 1

    ELSE

    0

  END

    ) AS exits,

  COUNT(DISTINCT(ga_session_id)) AS total_sessions,

  SUM(CASE

      WHEN row_num = 1 THEN 1

    ELSE

    0

  END

    ) / COUNT(DISTINCT(ga_session_id)) AS exit_rate

FROM

  cte

GROUP BY

  1

ORDER BY

  3 DESC
