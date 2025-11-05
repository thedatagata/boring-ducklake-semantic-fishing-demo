{{
  config(
    materialized='table',
    strategy='append'
  )
}}

WITH 
    sessions_base 
        AS 
            (
                SELECT
                    _dlt_id as session_key,
                    visit_id as session_id,
                    full_visitor_id as user_id,
                    CAST(visit_number as INTEGER) as session_number,
                    TO_TIMESTAMP(CAST(visit_start_time AS BIGINT)) - INTERVAL '7' MONTH + INTERVAL '7' YEAR as session_start_time,
                    date as session_date,
                    json_extract_string(device, '$.browser') as session_device__browser,
                    json_extract_string(device, '$.operatingSystem') as session_device__os,
                    json_extract_string(device, '$.deviceCategory') as session_device__device_category,
                    CAST(json_extract_string(device, '$.isMobile') as BOOLEAN) as session_device__is_mobile,
                    json_extract_string(geo_network, '$.continent') as session_geo__continent,
                    json_extract_string(geo_network, '$.subContinent') as session_geo__sub_continent,
                    json_extract_string(geo_network, '$.country') as session_geo__country,
                    CAST(json_extract_string(totals, '$.visits') as INTEGER) as session_totals__visits,
                    CAST(json_extract_string(totals, '$.hits') as INTEGER) as session_totals__hits,
                    CAST(json_extract_string(totals, '$.pageviews') as INTEGER) as session_totals__pageviews,
                    CAST(json_extract_string(totals, '$.timeOnSite') as INTEGER) as session_totals__time_on_site,
                    CAST(json_extract_string(totals, '$.newVisits') as INTEGER) as session_totals__new_visits,
                    CAST(json_extract_string(totals, '$.transactionRevenue') as BIGINT) as session_totals__transaction_revenue,
                    json_extract_string(traffic_source, '$.referralPath') as session_traffic_source__referrer,
                    json_extract_string(traffic_source, '$.source') as session_traffic_source__source,
                    json_extract_string(traffic_source, '$.medium') as session_traffic_source__medium,
                    json_extract_string(traffic_source, '$.campaign') as session_traffic_source__campaign
                FROM {{source('duck_pond', 'load')}}
            ), 
    deduped_sessions 
        AS 
            (
                {{ dbt_utils.deduplicate(
                    relation='sessions_base',
                    partition_by='user_id, session_id',
                    order_by='session_start_time'
                )}}
            )
SELECT * 
FROM deduped_sessions