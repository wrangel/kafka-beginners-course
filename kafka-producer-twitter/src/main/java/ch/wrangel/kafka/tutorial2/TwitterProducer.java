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

import static ch.wrangel.kafka.tutorial2.Constants.*;


// Much of the class is based on quickstart from https://github.com/twitter/hbc
public class TwitterProducer {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    // Optional: set up some track terms
    private final List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");

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

        // Create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // All below configs are not necessary, since included implicitly in idempotence
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        /* High throughput producer (at the expense of a bit of latency and CPU usage)
        https://blog.cloudflare.com/squeezing-the-firehose/
         */
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // Introduce producer delay
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size

        return new KafkaProducer<>(properties);
    }

}
