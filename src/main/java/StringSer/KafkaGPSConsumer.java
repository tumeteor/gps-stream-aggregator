package StringSer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.json.JSONObject;
import osrm.Match;
import osrm.Utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class KafkaGPSConsumer {

    final static Match osrm_match = new Match();

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
//        props.put("key.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id",     "test-group");
        props.put("kafka.topic"     , "gps-trace-output");



        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props, windowedSerde.deserializer(), Serdes.String().deserializer());


        consumer.subscribe(Arrays.asList(props.getProperty("kafka.topic")));

        System.out.println("Subscribed to topic " + props.getProperty("kafka.topic"));

        runMainLoop(consumer);


    }

    static void runMainLoop(KafkaConsumer<String, String> consumer) {

        try{
            while (true){
                ConsumerRecords records = consumer.poll(Duration.ofMillis(3));

                System.out.println(records.count());


                Iterator<ConsumerRecord<String, String>> recordItr = records.iterator();

                while (recordItr.hasNext()) {

                    ConsumerRecord<String, String> record = recordItr.next();

                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));

                    System.out.println("value: " + record.value().trim());


                    if (record != null || record.value().trim() != "") {
                        /**
                         * send data to OSRM for map-matching
                         */

                        System.out.println("start map matching");

                        JSONObject map_matched = osrm_match.matchPoints(Utils.parseCoordinate(record.value()));

                        if (map_matched != null)
                            System.out.println(Utils.toPrettyFormat(map_matched));
                    }

                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            consumer.close();
        }
    }


}
