package streams.processor;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import streams.Probe;
import streams.config.ConfigVars;

import java.util.Collections;

public class AvroGPSAggregator extends BaseAggregator<GenericRecord> {
    final String schemaRegistryUrl = System.getenv(ConfigVars.SCHEMA_REGISTRY_HOST);
    public AvroGPSAggregator() {
        super();
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Where to find the schema registry instance(s)
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    }

    /***
     * Parsing raw GPS tuple, SUBJECTIVE to change
     * @param tuple, in the form of vehicleID,lan,lon
     * @return
     */
    public String parseTuple(GenericRecord tuple) {
        String lon = tuple.get(Probe.LON).toString();
        String lat = tuple.get(Probe.LAT).toString();
        return lon + "," + lat;
    }

    @Override
    public Boolean isValidMessage(GenericRecord tuple) {
        return Boolean.TRUE;
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

        Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.
                SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        final StreamsBuilder builder = new StreamsBuilder();
        // Read the input Kafka topic into a KStream instance.
        final KStream<String, GenericRecord> gpsLines = builder.stream(inTopic, Consumed.with(stringSerde, genericAvroSerde));
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        gpsLines.filter((key, value) -> isValidMessage(value))
                .mapValues(v -> parseTuple(v))
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
                .toStream()
                .to(outTopic, Produced.with(windowedSerde, Serdes.String()));

        return builder.build();
    }

    public static void main (String[] args) {
        AvroGPSAggregator agg = new AvroGPSAggregator();
        agg.run(ConfigVars.AVRO_TOPIC_GPS_PRODUCER, ConfigVars.AVRO_TOPIC_GPS_AGG);
    }
}
