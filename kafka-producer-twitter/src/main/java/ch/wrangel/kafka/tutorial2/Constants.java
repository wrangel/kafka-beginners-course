package ch.wrangel.kafka.tutorial2;

public class Constants {

    public static final String bootstrapServers = "localhost:9092";
    /* Important: Create topic before starting producer:
        kafka-topics --bootstrap-server localhost:9092 --topic twitter-tweets --partitions 6 --replication-factor 1 --create
        Plus, add a consumer
        kafka-console-consumer --bootstrap-server localhost:9092 --topic twitter-tweets
    */
    public static final String topic = "twitter-tweets";

    // Twitter access
    public static final String consumerKey = "Y4mNgLCYmatR09RbJaeEIHXbU";
    public static final String consumerSecret = "3SELqV5ffz9dRp00pymgalx0eOYYYQou7cikK8ccR0JvjgJ33V";
    public static final String token = "1296708112515301376-CTzHspACtUJ2g2bOUWtrknNmq8F7Oq";
    public static final String secret = "M0w6yees60pekkdrfmJ0iKi7jjPGQ45stZGQQbAfUFIyW";


}
