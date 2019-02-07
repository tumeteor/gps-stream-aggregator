package StringSer;

import io.minio.errors.MinioException;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;
import osrm.Match;
import osrm.Utils;
import persistence.Minio;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

public class KafkaGPSConsumer {

    final static Match osrm_match = new Match(System.getenv("ROUTING"));
    private static final boolean RUNNING = true;
    private static final Object CONSUMER_GROUP = "gps-group";
    private static final String BUCKET_NAME = "gps-s3-bucket";

    private static Logger log = LoggerFactory.getLogger("DemoCallBack");

    private static Minio minio = new Minio();


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv("KAFKA_HOST"));
        props.put("group.id",  CONSUMER_GROUP);
        props.put("kafka.topic"     , "gps-trace-output");

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props, windowedSerde.deserializer(), Serdes.String().deserializer());
        consumer.subscribe(Arrays.asList(props.getProperty("kafka.topic")));
        log.info("Subscribed to topic " + props.getProperty("kafka.topic"));

        /*
         * create bucket in s3
         */
        try {
            minio.make_bucket("");
            runMainLoop(consumer);
        } catch (MinioException e) {
            e.printStackTrace();
        } catch (XmlPullParserException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static void runMainLoop(KafkaConsumer<String, String> consumer) {
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
                        JSONObject batchMm = null;
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
                                if (batchMm == null) batchMm = map_matched;
                                else batchMm = Utils.mergeJSONObjects(batchMm, map_matched);

                                // send to Minio
                                minio.write_to_s3(BUCKET_NAME, new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()), batchMm);

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
