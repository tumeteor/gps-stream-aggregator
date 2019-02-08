package stringser.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.kstream.Windowed;
import org.json.JSONObject;
import osrm.Utils;

import java.time.Duration;
import java.util.Iterator;

public class KafkaGPSConsumer extends AbstractConsumer {

    public static void main(String[] args) {
        KafkaGPSConsumer gpsConsumer = new KafkaGPSConsumer();
        gpsConsumer.run();
    }

    void runMainLoop(KafkaConsumer<String, String> consumer) {
        try{
            while (RUNNING){
                ConsumerRecords records = consumer.poll(Duration.ofMillis(3));
                log.info("number of records: " + records.count());
                Iterator<ConsumerRecord<Windowed<String>, String>> recordItr = records.iterator();

                try {
                    while (recordItr.hasNext()) {
                        ConsumerRecord<Windowed<String>, String> record = recordItr.next();
                        log.info(String.format("Topic - %s, Partition - %d, Key: %s, Value: %s", record.topic(), record.partition(),
                                record.key(), record.value()));
                        /*
                         * Concatenate map-matched results in JSON
                         * for batch-persistence
                         */
                        if (record != null) {
                            /*
                             * send data to OSRM for map-matching
                             * NOTE: as data is streamed with 'fake' timestamps,
                             * WindowedBy at Aggregation does not work, OSRM can't handle too long trace.
                             * Dummy Trick: for testing the flow, break down the trace now.
                             */

                            log.info("start map-matching");

                            JSONObject map_matched = osrm_match.matchPoints(Utils.parseCoordinate(record.value()));
                            if (map_matched != null){
                                log.info(Utils.toPrettyFormat(map_matched));
                            }
                        }
                    }
                } catch (CommitFailedException e) {
                    // application specific failure handling
                }
                consumer.commitSync();
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
