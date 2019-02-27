package streams.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import streams.config.ConfigVars;

import java.util.Properties;

public abstract class BaseAggregator<T> {
    protected static final long SIZE_MS = 3 * 60;
    protected Properties props;

    public BaseAggregator() {
        props = new Properties();

        /*
         *  Give the Streams application a unique name.  The name must be unique in the Kafka cluster
         *  against which the application is run.
         */
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "GPS-trace-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "GPS-trace-client");

        // Where to find Kafka broker(s).
        if (System.getenv(ConfigVars.KAFKA_HOST) != null)
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(ConfigVars.KAFKA_HOST));
        else
            props.put("bootstrap.servers", "localhost:9092");
        // Specify default (de)serializers for record keys and for record values.

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    }

    /**
     *
     * @param inTopic
     * @param outTopic
     * @return
     */
    public abstract Topology createTopology(String inTopic, String outTopic);

    /**
     * Parsing message tuple for aggregation
     * @param tuple
     * @return
     */
    public abstract String parseTuple(T tuple);

    /**
     * Sanity checking incoming messages
     * @param tuple
     * @return
     */
    public abstract Boolean isValidMessage(T tuple);

    public void run(String inTopic, String outTopic) {
        KafkaStreams streams = new KafkaStreams(createTopology(inTopic, outTopic), props);
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Update:
        // print the topology every 10 seconds for tracing purposes
        while(true){
            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }


}
