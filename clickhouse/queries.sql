-- Compare the ingestion paths.
SELECT 'Kafka engine: raw events' AS path, count() AS rows FROM search_demo.raw_events
UNION ALL
SELECT 'Flink + Connect: human clicks', count() FROM search_demo.search_clicks;

-- Popular queries and result positions after Flink normalization.
SELECT query_normalized, count() AS clicks,
       round(avg(result_position), 2) AS average_position,
       countIf(position_bucket = 'top_3') AS top_3_clicks
FROM search_demo.search_clicks
GROUP BY query_normalized
ORDER BY clicks DESC;

-- Human click-through rate, using raw events from the Kafka engine.
SELECT lower(trimBoth(query_text)) AS query,
       countIf(event_type = 'search_impression') AS impressions,
       countIf(event_type = 'result_click') AS clicks,
       round(clicks / nullIf(impressions, 0), 3) AS ctr
FROM search_demo.raw_events
WHERE is_bot = 0
GROUP BY query;
