package ch.wrangel.kafka.tutorial3;

public class Constants {

    public static final String bootstrapServers = "localhost:9092";
    /* Important: Create topic before starting producer:
        kafka-topics --bootstrap-server localhost:9092 --topic twitter-tweets --partitions 6 --replication-factor 1 --create
        Plus, add a consumer
        kafka-console-consumer --bootstrap-server localhost:9092 --topic twitter-tweets
    */
    public static final String topic = "twitter-tweets";

    // Use info provided in "Access" tab on bonsai.io
    public static final String elasticSearchAccess =
            "https://ass4h85py6:t92ddqc7nb@kafka-course-5695759870.eu-central-1.bonsaisearch.net:443";


}
