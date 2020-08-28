package ch.wrangel.kafka.tutorial3;

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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
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
            for (ConsumerRecord<String, String> record : records) {
                // Insert data into ElasticSearch
                IndexRequest indexRequest = new IndexRequest(topic)
                        .source(record.value(), XContentType.JSON);
            /* Request will fail unless twitter index exists
                Create twitter index in bonsai.io console: PUT /twitter
            */
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();

                // Get back the ES id from the jsonString
                logger.info(id);
            }
        }

        // Close the client gracefully
        // client.close();
    }

    // Create elasticsearch client
    private static RestHighLevelClient createClient() {

        // Use info provided in "Access" tab on bonsai.io
        final String elasticSearchAccess =
                "https://ass4h85py6:t92ddqc7nb@kafka-course-5695759870.eu-central-1.bonsaisearch.net:443";

        final int index1 = elasticSearchAccess.indexOf("://");
        final int index2 = elasticSearchAccess.indexOf(":", index1 + 1);
        final int index3 = elasticSearchAccess.indexOf("@");
        final int index4 = elasticSearchAccess.lastIndexOf(":");

        final String scheme = elasticSearchAccess.substring(0, index1);
        final String username = elasticSearchAccess.substring(index1 + 3, index2);
        final String password = elasticSearchAccess.substring(index2 + 1, index3);
        final String hostname = elasticSearchAccess.substring(index3 + 1, index4);
        final int port = Integer.parseInt(elasticSearchAccess.substring(index4 + 1));

        // Don't do this if you're running a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, port, scheme))
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

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

}
