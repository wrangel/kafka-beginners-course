package ch.wrangel.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
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
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //3) Create producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                "first-topic",
                 "hello-world"
        );

        // 4) Send data
        producer.send(record);

        // To execute, start a kafka-console-consumer:
        // kafka-consumer-groups --bootstrap-server localhost:9092 --group my-third-application

    }
}
