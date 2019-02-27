package streams.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class KafkaGPSAggregator extends BaseAggregator<String> {

    public KafkaGPSAggregator() {
        super();
        // Specify default (de)serializers for record values of String
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

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

    @Override
    public Boolean isValidMessage(String tuple) {
        return (tuple.split(",").length == 4);
    }

    /**
     * Creating stream processor topology
     * @return topology
     */
    public Topology createTopology(String inTopic, String outTopic) {
        // Set up serializers and deserializers, which we will use for overriding the default serdes
        // specified above.
        final Serde<String> stringSerde = Serdes.String();
        // In the subsequent lines we define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();
        // Read the input Kafka topic into a KStream instance.
        final KStream<String, String> gpsLines = builder.stream(inTopic, Consumed.with(stringSerde, stringSerde));
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        gpsLines.filter((key, value) -> isValidMessage(value))
                .mapValues(v -> parseTuple(v))
                // flatmap values (geo loc), split by tab
                //.flatMapValues(textLine -> Arrays.asList(textLine.split("\t")))
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
                .toStream().to(outTopic, Produced.with(windowedSerde, Serdes.String()));

        return builder.build();
    }

    public static void main (String[] args) {
        KafkaGPSAggregator aggregator = new KafkaGPSAggregator();
        aggregator.run("gps-topic1", "gps-trace-output1");
    }
}
