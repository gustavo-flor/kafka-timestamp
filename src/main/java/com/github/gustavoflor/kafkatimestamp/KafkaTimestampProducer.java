package com.github.gustavoflor.kafkatimestamp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaTimestampProducer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        // Set up producer properties
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.github.gustavoflor.kafkatimestamp.serialization.JsonSerializer");

        // Create a Kafka producer instance
        final Producer<String, JsonNode> producer = new KafkaProducer<>(props);

        // Create a producer record with a timestamp
        final ProducerRecord<String, JsonNode> thirtySecondsAgoMessage = getMessage(System.currentTimeMillis() - 30000); // 30 seconds ago
        final ProducerRecord<String, JsonNode> currentTimeMessage = getMessage(System.currentTimeMillis()); // current time
        final ProducerRecord<String, JsonNode> twoMinutesAheadMessage = getMessage(System.currentTimeMillis() + 120000); // 2 minutes ahead

        // Send the records to the Kafka broker
        producer.send(thirtySecondsAgoMessage);
        producer.send(currentTimeMessage);
        producer.send(twoMinutesAheadMessage);

        // Close the producer
        producer.close();
    }

    private static ProducerRecord<String, JsonNode> getMessage(final long timestamp) {
        final Map<String, String> map = new HashMap<>();
        map.put("created_at", DateFormat.getDateTimeInstance().format(new Date()));
        final JsonNode value = OBJECT_MAPPER.valueToTree(map);
        return new ProducerRecord<>("my-topic", null, timestamp, null, value);
    }

}
