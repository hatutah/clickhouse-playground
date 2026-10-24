FROM alpine:3.21.3 AS plugin
RUN wget -q -O /tmp/connector.zip https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v1.3.7/clickhouse-kafka-connect-v1.3.7.zip \
    && unzip /tmp/connector.zip -d /plugin
RUN wget -q -O /tmp/protobuf.zip https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-protobuf-converter/versions/7.9.0/archive \
    && unzip /tmp/protobuf.zip -d /protobuf

FROM apache/kafka:3.9.1
COPY --from=plugin /plugin /opt/kafka/plugins/clickhouse
COPY --from=plugin /protobuf /opt/kafka/plugins/protobuf
COPY worker.properties /opt/kafka/config/worker.properties
ENTRYPOINT ["/opt/kafka/bin/connect-distributed.sh", "/opt/kafka/config/worker.properties"]
