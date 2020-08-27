package ch.wrangel.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static ch.wrangel.kafka.tutorial1.Constants.bootstrapServers;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);
        // Change group id in order to read from beginning / Way of resetting the application
        String groupId = "my-fifth-application";
        String topic = "second-topic";
        Duration timeout = Duration.ofMillis(100);

        // Create consumer configs
        Properties properties = new Properties();
        // Consult Kafka documentation, "New Consumer Properties"
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Take bytes and create String (deserialize)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Earliest: Read from beginning of topic ("--from-beginning" in CLI), latest: Read only new messages
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe consumer to topic(s) - with singleton, this will be one topic, use Arrays.asList("", "") for > 1 topic
        consumer.subscribe(Collections.singleton(topic));

        // Poll for data - Consumer does not get data until it asks for it
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            for(ConsumerRecord<String, String> record : records)
                logger.info("Received new metadata: " +
                        ", Key: " + record.key() +
                        ", Value: " + record.value() +
                        ", Partition: " + record.partition() +
                        ", Offset: " + record.offset()
                );
        }
    }
}
