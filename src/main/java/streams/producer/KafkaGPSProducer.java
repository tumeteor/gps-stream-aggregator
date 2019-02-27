package streams.producer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import streams.config.ConfigVars;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;

/**
 * Simulation of a GPS producer that continuously send GPS data
 * of Vehicles in the form of (VehicleID, lon, lat, timestamp).
 * The stream is sent does not strictly (or at all) follow the timestamp in this
 * simulator.
 */

public class KafkaGPSProducer extends BaseTextProducer<String> {

    public KafkaGPSProducer(String topic, Boolean isAsync, Boolean onK8s) {
        super(topic, isAsync, onK8s, SERIALIZER.STRINGSE);
    }

    public static void main(String [] args){
        Options options = new Options();

        Option isOnK8s = new Option("k", "onK8S", true, "on K8S mode");
        isOnK8s.setRequired(false);
        options.addOption(isOnK8s);
        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cmd = parser.parse(options, args);
            KafkaGPSProducer producer;
            if (cmd.getOptionValue("onK8S") == null) {
                producer = new KafkaGPSProducer(ConfigVars.STRG_TOPIC_GPS_PRODUCER, false, false);
                producer.log.info("reading from local");
                producer.readFromLocal();
            } else {
                producer = new KafkaGPSProducer(ConfigVars.STRG_TOPIC_GPS_PRODUCER, false, true);
                producer.log.info("reading from S3");
                producer.readFromS3(BUCKET, BUCKET_KEY);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally{
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Read sample file from S3
     * @param bucketName
     * @param key
     * @throws IOException
     */
    public void readFromS3(String bucketName, String key) throws IOException {
        String accessKey = System.getenv(ConfigVars.AWS_ACCESS_KEY);
        String secretKey = System.getenv(ConfigVars.AWS_SECRET_KEY);
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        log.info("Accessed to s3 region: " + s3Client.getRegionName());
        S3Object s3object = s3Client.getObject(new GetObjectRequest(
                bucketName, key));

        br = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
        while (true) {
            final String line = br.readLine();
            //TRICK to get the stream run continuously
            //reset the stream
            if (line == null) {
                br = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
                continue;
            }
            String[] tuple = line.split(",");
            sendMessage(tuple[0], line);
        }
    }

    /**
     * Read sample file from local
     * @throws IOException
     */
    public void readFromLocal() throws IOException {
        InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream(fileName);

        //Construct BufferedReader from InputStreamReader
        br = new BufferedReader(new InputStreamReader(is));

        while (true) {
            final String line = br.readLine();
            //TRICK to get the stream run continuously
            //reset the stream
            if (line == null) {
                is = ClassLoader.getSystemClassLoader().getResourceAsStream(fileName);
                br = new BufferedReader(new InputStreamReader(is));
                continue;
            }
            String[] tuple = line.split(",");
            sendMessage(tuple[0], line);
        }
    }

    @Override
    /**
     * A method to send message (key, value) to Kafka
     * @param key
     *        the key is in String type
     * @param value
     *        the value is in String type
     */
    public void sendMessage(String key, String value) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            producer.send(
                    new ProducerRecord<>(topicName, key),
                    new ProducerCallBack(startTime, key, value));
        } else { // Send synchronously
            try {
                producer.send(
                        new ProducerRecord<>(topicName, key, value))
                        .get();
                log.info("Sent message: (" + key + ", " + value + ")");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}

