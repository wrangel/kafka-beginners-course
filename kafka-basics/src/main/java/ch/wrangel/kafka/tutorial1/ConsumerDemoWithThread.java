package ch.wrangel.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static ch.wrangel.kafka.tutorial1.Constants.bootstrapServers;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    // Constructor
    private ConsumerDemoWithThread() {
    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        String groupId = "my-sixth-application";
        String topic = "second-topic";

        // Create latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Create the consumer runnable
        logger.info("Creating the consumer thread");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(topic, groupId, latch);

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // Add shutdown hook / Properly shutdown application
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("Cought shutdown hook");
                    myConsumerRunnable.shutdown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        logger.info("Application has exited");
                    }
                }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public static class ConsumerRunnable implements Runnable {
        // Latch will be able to shutdown application correctly
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        // Constructor
        // Latch: Deal with concurrency
        public ConsumerRunnable(
                String topic,
                String groupId,
                CountDownLatch latch
        ) {
            this.latch = latch;
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
            consumer = new KafkaConsumer<>(properties);
            // Subscribe consumer to topic(s) - with singleton, this will be one topic, use Arrays.asList("", "") for > 1 topic
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            // Poll for data - Consumer does not get data until it asks for it
            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String, String> record : records)
                        logger.info("Received new metadata: " +
                                ", Key: " + record.key() +
                                ", Value: " + record.value() +
                                ", Partition: " + record.partition() +
                                ", Offset: " + record.offset()
                        );
                }
            } catch (WakeupException w) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // Tell our main code we're done with the consumer
                latch.countDown();
            }

        }

        public void shutdown() throws WakeupException    {
            // The wakeup method is a special method to interrupt consumer.poll()
            consumer.wakeup();
        }
    }
}
