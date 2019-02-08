package stringser.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import osrm.Match;

import java.util.Arrays;
import java.util.Properties;

public abstract class AbtractConsumer {

    protected static final boolean RUNNING = true;
    protected static final Object CONSUMER_GROUP = "gps-group";
    protected final static Match osrm_match = new Match(System.getenv("ROUTING"));

    protected final Logger log = LoggerFactory.getLogger(getClass());


    protected KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv("KAFKA_HOST"));
        props.put("group.id",  CONSUMER_GROUP);
        props.put("kafka.topic"     , "gps-trace-output");

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props, windowedSerde.deserializer(), Serdes.String().deserializer());
        consumer.subscribe(Arrays.asList(props.getProperty("kafka.topic")));
        log.info("Subscribed to topic " + props.getProperty("kafka.topic"));
        return consumer;
    }

    protected void run() {
        runMainLoop(createConsumer());
    }

    abstract void runMainLoop(KafkaConsumer<String, String> consumer);


}
