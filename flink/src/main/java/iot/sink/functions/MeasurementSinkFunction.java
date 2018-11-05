package iot.sink.functions;

import iot.NotificationManager;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import javax.mail.MessagingException;
import java.util.HashMap;
import java.util.Map;

import static iot.StreamingJob.addFloatIfExistsInObject;
import static iot.StreamingJob.addIfExistsInObject;
import static iot.StreamingJob.getThreshold;

public class MeasurementSinkFunction implements ElasticsearchSinkFunction<ObjectNode> {
    private final NotificationManager notificationManager = new NotificationManager();


    IndexRequest createThresholdTransgressionRequest(JsonNode content) {
        Map<String, Map<String, Object>> json = new HashMap<>();
        Map<String, Object> data = new HashMap<>();

        float temperature = content.get("temperature").floatValue();

        data.put("timestamp", content.get("timestamp"));
        data.put("threshold", getThreshold());
        data.put("value", temperature);

        json.put("data", data);

        return Requests.indexRequest()
                .index("measurements")
                .type("notification")
                .source(json);
    }

    IndexRequest createIndexRequest(JsonNode content) {
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
                .type("measurements")
                .source(json);
    }

    @Override
    public void process(ObjectNode element, RuntimeContext ctx, RequestIndexer indexer) {
        JsonNode content = element.get("value");

        if (content.hasNonNull("timestamp") && content.get("timestamp").isTextual()) {
            indexer.add(createIndexRequest(content));

            if (content.hasNonNull("temperature") && content.get("temperature").isFloat()) {
                float temperature = content.get("temperature").floatValue();

                if (temperature > getThreshold()) {
                    indexer.add(createThresholdTransgressionRequest(content));

                    try {
                        notificationManager.sendNotification(getThreshold(), temperature);
                    } catch (MessagingException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }
}
