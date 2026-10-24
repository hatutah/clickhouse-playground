CREATE DATABASE IF NOT EXISTS search_demo;

CREATE TABLE IF NOT EXISTS search_demo.raw_events
(
    event_id String,
    run_id String,
    event_type LowCardinality(String),
    event_time_ms UInt64,
    search_id String,
    session_id String,
    user_id String,
    query_text String,
    result_id String,
    result_position UInt32,
    is_bot UInt8
)
ENGINE = MergeTree
ORDER BY (run_id, event_type, event_time_ms, event_id);

CREATE TABLE IF NOT EXISTS search_demo.kafka_events
AS search_demo.raw_events
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'search_events',
         kafka_group_name = 'clickhouse-raw-events',
         kafka_format = 'ProtobufSingle',
         kafka_schema = 'search_event.proto:SearchEvent',
         kafka_schema_registry_skip_bytes = 6,
         kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS search_demo.raw_events_mv
TO search_demo.raw_events AS SELECT * FROM search_demo.kafka_events;

CREATE TABLE IF NOT EXISTS search_demo.search_clicks
(
    event_id String,
    run_id String,
    event_time_ms UInt64,
    search_id String,
    session_id String,
    user_id String,
    query_normalized String,
    result_id String,
    result_position UInt32,
    position_bucket LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (run_id, query_normalized, event_time_ms, event_id);
