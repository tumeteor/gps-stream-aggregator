package StringSer.consumer;

import io.minio.errors.MinioException;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.kstream.Windowed;
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
import java.util.Date;
import java.util.Iterator;

public class KafkaGPSConsumer extends AbtractConsumer {

    final static Match osrm_match = new Match(System.getenv("ROUTING"));

    /*
     * bucket name must be at least 3 and no more than 63 characters long
     */
    private static final String BUCKET_NAME = "gps-s3-bucket";

    private static Logger log = LoggerFactory.getLogger("DemoCallBack");

    private static Minio minio = new Minio();




    public static void main(String[] args) {
        try {
            /*
             * create bucket in s3
             */
            minio.make_bucket(BUCKET_NAME);
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
