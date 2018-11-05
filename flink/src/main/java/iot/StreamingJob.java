/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package iot;

import iot.sink.functions.MeasurementSinkFunction;
import iot.sink.functions.ThresholdChangeSinkFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
    private static float threshold = 10f;

    public static void setThreshold(float newThreshold) {
        threshold = newThreshold;
    }

    public static float getThreshold() {
        return threshold;
    }

    public static void addFloatIfExistsInObject(JsonNode node, String key, Map<String, Object> map) {
        if (node.hasNonNull(key)) {
            map.put(key, node.get(key).floatValue());
        }
    }

    public static void addIfExistsInObject(JsonNode node, String key, Map<String, Object> map) {
        if (node.hasNonNull(key)) {
            map.put(key, node.get(key).asText());
        }
    }

    private static void setupStream(StreamExecutionEnvironment env, Map<String, String> config, List<InetSocketAddress> transportAddresses, Properties properties) {
        FlinkKafkaConsumer011<ObjectNode> measurementsConsumer = new FlinkKafkaConsumer011<>("measurements", new JSONKeyValueDeserializationSchema(false), properties);
        FlinkKafkaConsumer011<ObjectNode> thresholdChangeConsumer = new FlinkKafkaConsumer011<>("threshold_change", new JSONKeyValueDeserializationSchema(false), properties);

        DataStream<ObjectNode> measurementsStream = env.addSource(measurementsConsumer);
        DataStream<ObjectNode> thresholdChangeStream = env.addSource(thresholdChangeConsumer);
        ConnectedStreams<ObjectNode, ObjectNode> streams = measurementsStream.connect(thresholdChangeStream);

        streams.getFirstInput().addSink(new ElasticsearchSink<>(config, transportAddresses, new MeasurementSinkFunction()));
        streams.getSecondInput().addSink(new ElasticsearchSink<>(config, transportAddresses, new ThresholdChangeSinkFunction()));
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka-internal:9092");
        properties.setProperty("group.id", "test");

        Map<String, String> config = new HashMap<>();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "docker-cluster");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("elasticsearch"), 9300));

        setupStream(env, config, transportAddresses, properties);

        // execute program
        env.execute("Room Environment Monitoring");
    }
}
