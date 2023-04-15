package com.github.gustavoflor.kafkatimestamp;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaTimestampConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTimestampConsumer.class);

    public static void main(String[] args) {
        // Set up consumer properties
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.github.gustavoflor.kafkatimestamp.serialization.JsonDeserializer");

        // Create a Kafka consumer instance
        final KafkaConsumer<String, JsonNode> consumer = new KafkaConsumer<>(props);

        // Get a list of partitions for the topic
        final String topic = "my-topic";
        final List<TopicPartition> partitions = consumer.partitionsFor(topic)
                .stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();

        // Assign consumer to partitions
        consumer.assign(partitions);

        // Put auto-closeable consumer
        try (consumer) {
            // Seek to a timestamp on each partition
            final long timestamp = System.currentTimeMillis() - 30000; // 30 seconds ago
            partitions.forEach(partition ->
                    consumer.offsetsForTimes(Map.of(partition, timestamp))
                            .forEach((key, value) -> executeOffsetsForTimes(consumer, key, value)));
        }
    }

    private static void executeOffsetsForTimes(final KafkaConsumer<String, JsonNode> consumer,
                                               final TopicPartition topicPartition,
                                               final OffsetAndTimestamp offsetAndTimestamp) {
        // Poll for records that were produced before the specified timestamp
        consumer.seek(topicPartition, offsetAndTimestamp.offset());
        final ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(1000));
        LOGGER.info("Polled {} records", records.count());
        records.forEach(message -> LOGGER.info("Key: {}, Value: {}, Timestamp: {}", message.key(), message.value(), message.timestamp()));
        consumer.commitSync();
    }

}
