package demo;

import demo.events.SearchClick;
import demo.events.SearchEvent;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.Map;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class ProtoFunctions {
    public static class DecodeEvent extends ScalarFunction {
        private transient KafkaProtobufDeserializer<SearchEvent> decoder;
        private final String registryUrl;

        public DecodeEvent(String registryUrl) {
            this.registryUrl = registryUrl;
        }

        @Override
        public void open(FunctionContext context) {
            decoder = new KafkaProtobufDeserializer<>();
            decoder.configure(Map.of(
                    "schema.registry.url", registryUrl,
                    "specific.protobuf.value.type", SearchEvent.class.getName()), false);
        }

        @DataTypeHint("ROW<event_id STRING, run_id STRING, event_type STRING, event_time_ms BIGINT, "
                + "search_id STRING, session_id STRING, user_id STRING, query_text STRING, "
                + "result_id STRING, result_position INT, is_bot INT>")
        public Row eval(byte[] payload) {
            SearchEvent event = decoder.deserialize("search_events", payload);
            return Row.of(event.getEventId(), event.getRunId(), event.getEventType(),
                    event.getEventTimeMs(), event.getSearchId(), event.getSessionId(),
                    event.getUserId(), event.getQueryText(), event.getResultId(),
                    event.getResultPosition(), event.getIsBot());
        }

        @Override
        public void close() {
            decoder.close();
        }
    }

    public static class EncodeClick extends ScalarFunction {
        private transient KafkaProtobufSerializer<SearchClick> encoder;
        private final String registryUrl;

        public EncodeClick(String registryUrl) {
            this.registryUrl = registryUrl;
        }

        @Override
        public void open(FunctionContext context) {
            encoder = new KafkaProtobufSerializer<>();
            encoder.configure(Map.of(
                    "schema.registry.url", registryUrl,
                    "auto.register.schemas", false,
                    "use.latest.version", true), false);
        }

        public byte[] eval(String eventId, String runId, Long eventTimeMs, String searchId,
                           String sessionId, String userId, String queryNormalized,
                           String resultId, Integer resultPosition, String positionBucket) {
            SearchClick click = SearchClick.newBuilder()
                    .setEventId(eventId).setRunId(runId).setEventTimeMs(eventTimeMs)
                    .setSearchId(searchId).setSessionId(sessionId).setUserId(userId)
                    .setQueryNormalized(queryNormalized).setResultId(resultId)
                    .setResultPosition(resultPosition).setPositionBucket(positionBucket)
                    .build();
            return encoder.serialize("search_clicks", click);
        }

        @Override
        public void close() {
            encoder.close();
        }
    }
}
