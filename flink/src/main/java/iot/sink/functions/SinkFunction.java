package iot.sink.functions;

import iot.NotificationManager;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import javax.mail.MessagingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static iot.StreamingJob.addFloatIfExistsInObject;
import static iot.StreamingJob.addIfExistsInObject;

public class SinkFunction implements ElasticsearchSinkFunction<ObjectNode> {
    private final String from;
    private final String password;
    private final String to;

    private float threshold = 10f;

    public SinkFunction(String[] args) {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        this.from = parameterTool.get("GMAIL_FROM");
        this.password = parameterTool.get("GMAIL_PASSWORD");
        this.to = parameterTool.get("GMAIL_TO");
    }


    IndexRequest createThresholdTransgressionRequest(JsonNode content) {
        Map<String, Map<String, Object>> json = new HashMap<>();
        Map<String, Object> data = new HashMap<>();

        float temperature = content.get("temperature").floatValue();

        data.put("timestamp", content.get("timestamp").asText());
        data.put("threshold", threshold);
        data.put("value", temperature);

        json.put("data", data);

        return Requests.indexRequest()
                .index("measurements")
                .type("notification")
                .source(json);
    }

    private IndexRequest createMeasurementRequest(JsonNode content) {
        Map<String, Map<String, Object>> json = new HashMap<>();
        Map<String, Object> data = new HashMap<>();

        addFloatIfExistsInObject(content, "temperature", data);
        addFloatIfExistsInObject(content, "humidity", data);
        addFloatIfExistsInObject(content, "pressure", data);
        addFloatIfExistsInObject(content, "altitude", data);

        addIfExistsInObject(content, "timestamp", data);

        json.put("data", data);

        return Requests.indexRequest()
                .index("measurements")
                .type("measurement")
                .source(json);
    }

    private Optional<IndexRequest> createThresholdChangeRequest(JsonNode content) {
        if ((content.hasNonNull("timestamp") && content.get("timestamp").isTextual() &&
                content.hasNonNull("threshold") && content.get("threshold").isNumber()
        )) {
            Map<String, Map<String, Object>> json = new HashMap<>();
            Map<String, Object> data = new HashMap<>();

            float newThreshold = content.get("threshold").floatValue();

            this.threshold = newThreshold;

            data.put("threshold", newThreshold);
            data.put("timestamp", content.get("timestamp").asText());

            json.put("data", data);

            return Optional.of(Requests.indexRequest()
                    .index("measurements")
                    .type("threshold")
                    .source(json));
        } else {
            return Optional.empty();
        }
    }

    private void sendNotification(float threshold, double temperature) throws MessagingException {
        new NotificationManager(this.from, this.password, this.to).sendNotification(threshold, temperature);
    }

    private void handleMeasurementRequest(RequestIndexer indexer, JsonNode content) {
        if (content.hasNonNull("timestamp") && content.get("timestamp").isTextual()) {
            indexer.add(createMeasurementRequest(content));

            if (content.hasNonNull("temperature") && content.get("temperature").isNumber()) {
                double temperature = content.get("temperature").doubleValue();

                if (temperature > threshold) {
                    indexer.add(createThresholdTransgressionRequest(content));

                    try {
                        sendNotification(threshold, temperature);
                    } catch (MessagingException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void process(ObjectNode element, RuntimeContext ctx, RequestIndexer indexer) {
        JsonNode metadata = element.get("metadata");

        if (!metadata.isNull()) {
            String topic = metadata.get("topic").asText();
            JsonNode content = element.get("value");

            switch (topic) {
                case "measurements":
                    handleMeasurementRequest(indexer, content);
                    break;
                case "threshold_change":
                    createThresholdChangeRequest(content).ifPresent(indexer::add);
                    break;
            }
        }
    }
}
