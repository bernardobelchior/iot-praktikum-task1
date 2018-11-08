package iot.sink.functions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SinkFunctionTest {
    @Test
    public void testCreateThresholdTransgressionRequest() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        final ObjectNode root = mapper.createObjectNode();

        root.set("timestamp", mapper.convertValue("123", JsonNode.class));
        root.set("temperature", mapper.convertValue(12f, JsonNode.class));

        String[] args = {};

        IndexRequest request = new SinkFunction(args).createThresholdTransgressionRequest(root);

        mapper = new ObjectMapper();

        TypeReference<HashMap<String, HashMap<String, String>>> typeRef = new TypeReference<HashMap<String, HashMap<String, String>>>() { };

        Map<String, Map<String, String>> map = mapper.readValue(request.source().utf8ToString(), typeRef);
        Map<String, String> data = map.get("data");

        assertEquals(data.get("timestamp"), "123");
        assertEquals(Float.parseFloat(data.get("value")), 12f, 0.2);
        assertEquals(Float.parseFloat(data.get("threshold")), 10f, 0.1);
    }
}
