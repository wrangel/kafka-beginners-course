package ch.wrangel.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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

// Verify results on bonsai.io console: GET /twitter/_doc/<document_id>
public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        String jsonString = "{\"foo\": \"bar\"}";

        /* Request will fail unless twitter index exists
        Create twitter index in bonsai.io console: PUT /twitter
         */
        IndexRequest indexRequest = new IndexRequest("twitter")
                .source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();

        // Get back the ES id from the jsonString
        logger.info(id);

        // Close the client gracefully
        client.close();
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

}
