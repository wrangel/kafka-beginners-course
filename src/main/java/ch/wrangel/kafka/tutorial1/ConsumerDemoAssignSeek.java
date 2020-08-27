package ch.wrangel.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static ch.wrangel.kafka.tutorial1.Constants.bootstrapServers;

// Another way of writing an application: Read from wherever we want to read from
public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
        String topic = "second-topic";
        Duration timeout = Duration.ofMillis(100);

        // Create consumer configs
        Properties properties = new Properties();
        // Consult Kafka documentation, "New Consumer Properties"
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Take bytes and create String (deserialize)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Earliest: Read from beginning of topic ("--from-beginning" in CLI), latest: Read only new messages
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        /* Assign and seek are mostly used to replay data or fetch a specific message
        Assign: read from partition, seek: seek for offset
         */

        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Collections.singleton(partitionToReadFrom));

        // Seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMesssagesReadSoFar = 0;

        // Poll for data - Consumer does not get data until it asks for it
        while(keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            for (ConsumerRecord<String, String> record : records) {
                numberOfMesssagesReadSoFar += 1;
                logger.info("Received new metadata: " +
                        ", Key: " + record.key() +
                        ", Value: " + record.value() +
                        ", Partition: " + record.partition() +
                        ", Offset: " + record.offset()
                );
                if (numberOfMesssagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false; // Exit the while loop
                    break; // Exit the for loop
                }
            }
            logger.info("Exiting the application");
        }
    }
}
