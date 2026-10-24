FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY flink/pom.xml ./
COPY flink/src ./src
COPY schemas ./src/main/proto
RUN mvn --batch-mode --no-transfer-progress package

FROM flink:1.20.3-scala_2.12-java17
ADD --checksum=sha256:1086f3eee73d727e234860fcd03adafc0d76f2fc70a25d39c36427693fff749d https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/flink-sql-connector-kafka.jar
USER root
RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka.jar
USER flink
COPY --from=build /build/target/search-click-transform-1.0.0.jar /opt/flink/usrlib/search-click-transform.jar
COPY flink/submit.sh /opt/flink/submit.sh
