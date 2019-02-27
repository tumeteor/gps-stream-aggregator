package streams.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import osrm.Match;

import java.util.Arrays;
import java.util.Properties;

public abstract class AbstractConsumer {

    protected static final boolean RUNNING = true;
    protected static final Object CONSUMER_GROUP = "gps-group";
    protected final static Match osrm_match = (System.getenv("ROUTING") != null)? new Match(System.getenv("ROUTING"))
            : new Match();

    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected static String kafka_topic;

    public AbstractConsumer(String kafka_topic) {
        this.kafka_topic = kafka_topic;
    }


    protected KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        if (System.getenv("KAFKA_HOST") != null)
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_HOST"));
        else
            props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id",  CONSUMER_GROUP);
        props.put("kafka.topic"     , kafka_topic);
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props, windowedSerde.deserializer(), Serdes.String().deserializer());
        consumer.subscribe(Arrays.asList(props.getProperty("kafka.topic")));
        log.info("Subscribed to topic " + props.getProperty("kafka.topic"));
        return consumer;
    }

    protected void run() {
        runMainLoop(createConsumer());
    }

    /**
     * Abtract class to continuously listen to /
     * and extract data from Kafka topic
     * @param consumer
     */
    abstract void runMainLoop(KafkaConsumer<String, String> consumer);


}
