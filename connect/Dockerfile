FROM alpine:3.21.3 AS plugin
RUN wget -q -O /tmp/connector.zip https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v1.3.7/clickhouse-kafka-connect-v1.3.7.zip \
    && echo "9498e6da4e7467e46e8ad37ed85272428ca8501163112ddcd595b1a006a9c18d  /tmp/connector.zip" | sha256sum -c - \
    && unzip /tmp/connector.zip -d /plugin
RUN wget -q -O /tmp/protobuf.zip https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-protobuf-converter/versions/7.9.0/archive \
    && echo "55daa01acaf589a1a0331e43f64c3325ab4fad71ddd404173074bd81959ecbce  /tmp/protobuf.zip" | sha256sum -c - \
    && unzip /tmp/protobuf.zip -d /protobuf

FROM apache/kafka:3.9.1
COPY --from=plugin /plugin /opt/kafka/plugins/clickhouse
COPY --from=plugin /protobuf /opt/kafka/plugins/protobuf
COPY worker.properties /opt/kafka/config/worker.properties
ENTRYPOINT ["/opt/kafka/bin/connect-distributed.sh", "/opt/kafka/config/worker.properties"]
