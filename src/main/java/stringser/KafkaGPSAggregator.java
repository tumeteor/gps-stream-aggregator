package StringSer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class KafkaGPSAggregator {

    private static final long SIZE_MS = 3 * 60;

    public static void main (String[] args) {

        Properties props = new Properties();

        /*
         *  Give the Streams application a unique name.  The name must be unique in the Kafka cluster
         *  against which the application is run.
         */
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "GPS-trace-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "GPS-trace-client");

        // Where to find Kafka broker(s).
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_HOST"));
        // Specify default (de)serializers for record keys and for record values.

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        KafkaGPSAggregator aggProcessor = new KafkaGPSAggregator();

        KafkaStreams streams = new KafkaStreams(aggProcessor.createTopology(), props);
        streams.start();


        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Update:
        // print the topology every 10 seconds for learning purposes
        while(true){
            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }


    /***
     * Parsing raw GPS tuple, SUBJECTIVE to change
     * @param tuple, in the form of vehicleID,lan,lon
     * @return
     */
    public String parseTuple(String tuple) {
        String[] parts = tuple.split(",");
        String lat = parts[1];
        String lon = parts[2];
        return lon + "," + lat;
    }

    /**
     * Creating stream processor topology
     * @return topology
     */
    public Topology createTopology() {
        // Set up serializers and deserializers, which we will use for overriding the default serdes
        // specified above.
        final Serde<String> stringSerde = Serdes.String();
        // In the subsequent lines we define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();
        // Read the input Kafka topic into a KStream instance.
        final KStream<String, String> gpsLines = builder.stream("my-topic", Consumed.with(stringSerde, stringSerde));
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        gpsLines
                .mapValues(v -> parseTuple(v))
                // flatmap values (geo loc), split by tab
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\t")))
                // group by key (vehicle ID) before aggregation
                .groupByKey()
                .windowedBy(TimeWindows.of(SIZE_MS).until(SIZE_MS))
                //.reduce((aggValue, newValue) -> aggValue  + "/t" + newValue);
                .aggregate(new Initializer<String>() {
                    public String apply() {
                        return "";
                    }
                }, new Aggregator<String, String, String>() {
                    @Override
                    public String apply(String aggKey, String value, String aggregate) {
                        if (aggregate == "" || aggregate == null) return value;
                        else  return aggregate + "\t" + value;

                    }
                })
                .toStream().to("gps-trace-output", Produced.with(windowedSerde, Serdes.String()));

        return builder.build();
    }
}
