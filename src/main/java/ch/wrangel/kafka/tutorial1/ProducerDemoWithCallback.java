package ch.wrangel.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        // Create a logger for the class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers = "localhost:9092";

        // 1) Create producer properties
        // Consult Kafka Documentation - Producer configs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        /* Following both help producer know what type of value it is
        sending to Kafka, and how it is serialized to bytes. Kafka client will
        convert whatever we send to it to bytes
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2) Create the producer
        // Key and value are both Strings
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        IntStream.range(1, 500).forEach (
                i -> {
                    //3) Create producer record
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            "first-topic",
                            "hello-world " + i
                    );
                    // 4) Send data (asynchronous! - until here, the program will exit and messages will never be sent)
                    producer.send(record, (recordMetadata, e) -> {
                        // Executes every time a record is being sent successfully, or Exception is thrown
                        if(e == null) {
                            logger.info("Received new metadata.\n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp() + "\n"
                            );
                        } else
                            logger.error("Error while producing", e);
                    });
                }
        );

        // 5) Flush data / Execute
        producer.close();

        // To execute, start a kafka-console-consumer:
        // kafka-consumer-groups --bootstrap-server localhost:9092 --group my-third-application

    }
}
