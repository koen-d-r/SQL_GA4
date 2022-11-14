
WITH

  preparation AS (

  SELECT

    CONCAT(user_pseudo_id, '_', (

      SELECT

        value.int_value

      FROM

        UNNEST(event_params)

      WHERE

        key = 'ga_session_id')) AS unique_session_id,

    MAX(event_timestamp) - MIN(event_timestamp) AS time_on_site,

    COUNT(*) AS total_events,

    SUM(

    IF

      (event_name = 'page_view', 1, 0)) AS total_pageviews,

    SUM(

    IF

      (event_name = 'view_item', 1, 0)) AS total_pdp_pageviews,

    SUM(

    IF

      (event_name = 'add_to_cart', 1, 0)) AS total_adds_to_cart,

    SUM(

    IF

      (event_name = 'purchase', 1, 0)) AS total_purchases

  FROM

    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`

  GROUP BY

    1),

  calculate_z_scores AS (

  SELECT

    unique_session_id,

    (preparation.time_on_site - AVG(preparation.time_on_site) OVER()) / STDDEV(preparation.time_on_site) OVER() AS z_tos,

    (total_events - AVG(total_events) OVER()) / STDDEV(total_events) OVER() AS z_events,

    (total_pageviews - AVG(total_pageviews) OVER()) / STDDEV(total_pageviews) OVER() AS z_pageviews,

    (total_pdp_pageviews - AVG(total_pdp_pageviews) OVER()) / STDDEV(total_pdp_pageviews) OVER() AS z_pdp_pageviews,

    (total_adds_to_cart - AVG(total_adds_to_cart) OVER()) / STDDEV(total_adds_to_cart) OVER() AS z_adds_to_cart,

    (total_purchases - AVG(total_purchases) OVER()) / STDDEV(total_purchases) OVER() AS z_purchases,

  FROM

    preparation)

SELECT

  *,

  (z_tos + z_events + z_pageviews + z_pdp_pageviews + z_adds_to_cart + z_purchases) / 6 AS average_z_score

FROM

  calculate_z_scores

ORDER BY

  2 DESC
