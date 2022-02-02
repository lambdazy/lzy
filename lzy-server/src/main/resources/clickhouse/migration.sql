CREATE TABLE IF NOT EXISTS servant_queue
(
    timestamp String,
    level     String,
    thread    String,
    logger    String,
    message   String,
    servant   String
) ENGINE = Kafka('kafka.default.svc.cluster.local:9092', 'servant', 'clickhouse') SETTINGS
    kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS servant_logs
(
    timestamp String,
    level     String,
    thread    String,
    logger    String,
    message   String,
    servant   String
) ENGINE = Log;

CREATE TABLE IF NOT EXISTS server_queue
(
    timestamp String,
    level     String,
    thread    String,
    logger    String,
    message   String
) ENGINE = Kafka('kafka.default.svc.cluster.local:9092', 'server', 'clickhouse') SETTINGS
    kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS server_logs
(
    timestamp String,
    level     String,
    thread    String,
    logger    String,
    message   String
) ENGINE = Log;

CREATE MATERIALIZED VIEW IF NOT EXISTS server_consumer TO server_logs
AS
SELECT *
FROM server_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS consumer TO servant_logs
AS
SELECT *
FROM servant_queue;