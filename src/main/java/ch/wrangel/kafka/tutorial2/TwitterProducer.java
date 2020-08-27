package ch.wrangel.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static ch.wrangel.kafka.tutorial1.Constants.bootstrapServers;

// Much of the class is based on quickstart from https://github.com/twitter/hbc
public class TwitterProducer {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    // Optional: set up some track terms
    private final List<String> terms = Lists.newArrayList("kafka");

    // Constructor
    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        logger.info("Setup");

        // Parameter. Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        // Create a Twitter client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection
        client.connect();

        // Create a Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application");
            logger.info("Shutting down client for Twitter");
            client.stop();
            logger.info("Closing producer");
            producer.close();
            logger.info("Done!");
        }));

        // Loop to send tweets to Kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                // Everytime we receive a message, we want the kafka producer to send it
                /* Important: Create topic before starting producer:
        kafka-topics --bootstrap-server localhost:9092 --topic twitter-tweets --partitions 6 --replication-factor 1 --create
        Plus, add a consumer
        kafka-console-consumer --bootstrap-server localhost:9092 --topic twitter-tweets
     */
                String topic = "twitter-tweets";
                producer.send(
                        new ProducerRecord<>(topic, null, msg),
                        // Intercept errors
                        (recordMetadata, e) -> {
                            // In case of errors
                            if (e != null)
                                logger.error("Something bad happened", e);
                        }
                );
            }
        }
        logger.info("End of application");

    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        final String consumerKey = "Y4mNgLCYmatR09RbJaeEIHXbU";
        final String consumerSecret = "3SELqV5ffz9dRp00pymgalx0eOYYYQou7cikK8ccR0JvjgJ33V";
        final String token = "1296708112515301376-CTzHspACtUJ2g2bOUWtrknNmq8F7Oq";
        final String secret = "M0w6yees60pekkdrfmJ0iKi7jjPGQ45stZGQQbAfUFIyW";

        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")  // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        // Return client
        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

}
