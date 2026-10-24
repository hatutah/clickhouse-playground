package demo;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.call;

public class SearchClickJob {
    public static void main(String[] args) {
        String brokers = env("KAFKA_BROKERS", "kafka:29092");
        String registryUrl = env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
        TableEnvironment tables = TableEnvironment.create(
                EnvironmentSettings.inStreamingMode());
        tables.getConfig().getConfiguration()
                .setString("pipeline.name", "search-click-transform");
        tables.getConfig().getConfiguration()
                .setString("execution.checkpointing.interval", "10 s");

        tables.createTemporaryTable("search_events", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("payload", DataTypes.BYTES())
                        .build())
                .option("topic", "search_events")
                .option("properties.bootstrap.servers", brokers)
                .option("properties.group.id", "flink-search-transform")
                .option("scan.startup.mode", "group-offsets")
                .option("properties.auto.offset.reset", "earliest")
                .format("raw")
                .build());

        tables.createTemporaryTable("search_clicks", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("payload", DataTypes.BYTES())
                        .build())
                .option("topic", "search_clicks")
                .option("properties.bootstrap.servers", brokers)
                .option("sink.delivery-guarantee", "at-least-once")
                .format("raw")
                .build());

        tables.from("search_events")
                .select(call(new ProtoFunctions.DecodeEvent(registryUrl), $("payload")).as("event"))
                .select($("event").flatten())
                .as($("event_id"), $("run_id"), $("event_type"), $("event_time_ms"),
                        $("search_id"), $("session_id"), $("user_id"), $("query_text"),
                        $("result_id"), $("result_position"), $("is_bot"))
                .filter($("event_type").isEqual("result_click")
                        .and($("is_bot").isEqual(0))
                        .and($("result_position").isGreater(0)))
                .select($("event_id"), $("run_id"), $("event_time_ms"),
                        $("search_id"), $("session_id"), $("user_id"),
                        $("query_text").trim().lowerCase().as("query_normalized"),
                        $("result_id"), $("result_position"),
                        $("result_position").isLessOrEqual(3)
                                .then(lit("top_3"), lit("below_3")).as("position_bucket"))
                .select(call(new ProtoFunctions.EncodeClick(registryUrl),
                        $("event_id"), $("run_id"), $("event_time_ms"),
                        $("search_id"), $("session_id"), $("user_id"),
                        $("query_normalized"), $("result_id"), $("result_position"),
                        $("position_bucket")).as("payload"))
                .executeInsert("search_clicks");
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
