WITH

  preparation AS (

  SELECT

    user_pseudo_id,

    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,

    CONCAT(user_pseudo_id, '_', (

      SELECT

        value.int_value

      FROM

        UNNEST(event_params)

      WHERE

        key = 'ga_session_id')) AS unique_session_id,

    (

    SELECT

      value.int_value

    FROM

      UNNEST(event_params)

    WHERE

      key = 'ga_session_number') AS ga_session_number,

    (

    SELECT

      value.string_value

    FROM

      UNNEST(event_params)

    WHERE

      key = 'page_location') AS page_location

  FROM

    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`

  WHERE

    _TABLE_SUFFIX BETWEEN '20210125'

    AND '20210131'

    AND event_name = 'page_view')

SELECT

  *,

  MAX(ga_session_number) OVER(PARTITION BY user_pseudo_id) AS number_of_sessions,

  ROW_NUMBER() OVER(PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC) AS user_page_sequence,

  ROW_NUMBER() OVER(PARTITION BY unique_session_id ORDER BY event_timestamp ASC) AS session_page_sequence

FROM

  preparation

ORDER BY

  1,

  2
