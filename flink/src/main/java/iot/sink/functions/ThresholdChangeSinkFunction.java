package iot.sink.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

import static iot.StreamingJob.addIfExistsInObject;
import static iot.StreamingJob.setThreshold;

public class ThresholdChangeSinkFunction implements ElasticsearchSinkFunction<ObjectNode> {
    IndexRequest createIndexRequest(JsonNode content) {
        Map<String, Map<String, Object>> json = new HashMap<>();
        Map<String, Object> data = new HashMap<>();

        float newThreshold = content.get("threshold").floatValue();

        setThreshold(newThreshold);

        data.put("threshold", newThreshold);
        addIfExistsInObject(content, "timestamp", data);

        json.put("data", data);

        return Requests.indexRequest()
                .index("measurements")
                .type("threshold")
                .source(json);
    }

    @Override
    public void process(ObjectNode element, RuntimeContext ctx, RequestIndexer indexer) {
        JsonNode content = element.get("value");

        if (content.hasNonNull("timestamp") && content.get("timestamp").isTextual() && content.hasNonNull("threshold") && content.get("threshold").isFloat()) {
            indexer.add(createIndexRequest(content));
        }
    }

}
