package ch.wrangel.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static ch.wrangel.kafka.tutorial4.Constants.*;

/* Competitor to Flink, Spark
    Start a Kafka producer (on topic twitter-tweets) as well as a consumer (on topic important-tweets)
    https://medium.com/@stephane.maarek/the-kafka-api-battle-producer-vs-consumer-vs-kafka-connect-vs-kafka-streams-vs-ksql-ef584274c1e
 */
public class StreamsFilterTweets {
    public static void main(String[] args) {
        // Create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Equivalent to groupId
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, application);
        // Strings as keys
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        // Strings as values
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Input topic
        KStream<String, String> inputTopic = streamsBuilder.stream(topic);
        KStream<String, String> filteredStream = inputTopic.filter(
                // Filter for tweets which have a user with >10k followers
                (k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000
        );
        filteredStream.to("important-tweets");

        // Build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // Start our streams application
        kafkaStreams.start();


    }

    private static Integer extractUserFollowersInTweet(String tweetJson) {

        try {
            return JsonParser.parseString(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
