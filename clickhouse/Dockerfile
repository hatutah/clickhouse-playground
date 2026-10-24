FROM clickhouse/clickhouse-server:26.3.2.3
COPY --chown=clickhouse:clickhouse schemas /etc/clickhouse-server/format_schemas
COPY clickhouse/schemas.xml /etc/clickhouse-server/config.d/schemas.xml
