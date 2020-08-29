package ch.wrangel.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static ch.wrangel.kafka.tutorial3.Constants.bootstrapServers;
import static ch.wrangel.kafka.tutorial3.Constants.topic;

// Verify results on bonsai.io console: GET /twitter/_doc/<document_sid>
public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {

        Duration timeout = Duration.ofMillis(100);
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer(topic);

        // Poll for data - Consumer does not get data until it asks for it
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            int recordCount = records.count();
            logger.info("Received " + recordCount + " records");
            BulkRequest bulkRequest = new BulkRequest();

            // Insert tweets into ES, one at a time (synchronous)
            for (ConsumerRecord<String, String> record : records) {

                try {
                    // To make consumer idempotent: 2 strategies
                    // 1st strategy: Kafka generic ID
                    //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                    // 2nd strategy: Use Twitter feed specific id for ES as well
                    String id = extractIdFromTweet(record.value());
                    // Insert data into ElasticSearch
                    IndexRequest indexRequest = new IndexRequest(topic)
                            .source(record.value(), XContentType.JSON)
                            /* Check idempotence by re-running consumer again, and obtaining the same ids
                                Data is re-inserted thereby, but not as duplicate, but overwriting
                             */
                            .id(id);
                    // Add request to a bulk request (takes no time)
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.warn("Skipping bad data: " + record.value());
                }
            }

            // Process bulk only if there are records in it
            if (recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                // Committing the offsets only after running through records
                logger.info("Committing the offsets");
                consumer.commitSync();
                logger.info("Offsets have been committed");
            }
        }

        // Close the client gracefully
        // client.close();
    }

    // Create elasticsearch client
    private static RestHighLevelClient createClient() {

        List<String> elements = dissectElasticSearchAccessString();

        // Don't do this if you're running a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(elements.get(1), elements.get(2)));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(elements.get(3), Integer.parseInt(elements.get(4)), elements.get(0)))
                .setHttpClientConfigCallback(
                        httpAsyncClientBuilder ->
                                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        // Change group id in order to read from beginning / Way of resetting the application
        String groupId = "kafka-demo-elasticsearch";

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
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // Receive only a limit number of records at a time

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    private static String extractIdFromTweet(String tweetJson) {
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    // Extract scheme, username, password, hostname, port
    private static List<String> dissectElasticSearchAccessString() {
        int index1 = Constants.elasticSearchAccess.indexOf("://");
        int index2 = Constants.elasticSearchAccess.indexOf(":", index1 + 1);
        int index3 = Constants.elasticSearchAccess.indexOf("@");
        int index4 = Constants.elasticSearchAccess.lastIndexOf(":");
        return Arrays.asList(
                Constants.elasticSearchAccess.substring(0, index1),
                Constants.elasticSearchAccess.substring(index1 + 3, index2),
                Constants.elasticSearchAccess.substring(index2 + 1, index3),
                Constants.elasticSearchAccess.substring(index3 + 1, index4),
                Constants.elasticSearchAccess.substring(index4 + 1)
        );
    }

}
