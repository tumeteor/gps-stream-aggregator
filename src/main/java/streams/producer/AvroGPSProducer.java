package streams.producer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import streams.Probe;

import java.io.*;
import java.util.concurrent.ExecutionException;

/**
 * Simulation of a GPS producer that continuously send GPS data
 * of Vehicles in the form of (VehicleID, lon, lat, timestamp).
 * The stream is sent does not strictly (or at all) follow the timestamp in this
 * simulator.
 */
public class AvroGPSProducer extends BaseTextProducer<GenericRecord> {

    public AvroGPSProducer(String topic, Boolean isAsync, Boolean onK8s) {
        super(topic, isAsync, onK8s, SERIALIZER.AVROSE);
    }

    @Override
    /**
     * A method to send message (key, value) to Kafka
     * @param key
     *        the key is in String type
     * @param value
     *        the value is in String type
     */
    public void sendMessage(String key, GenericRecord value) {
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
                log.info("Sent message: (" + key + ", " + value.toString() + ")");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
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
        String accessKey = System.getenv("aws_access_key_id");
        String secretKey = System.getenv("aws_secret_access_key");
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

            GenericRecord probe = new GenericData.Record(loadAvroSchema());
            probe.put(Probe.KEY, tuple[0]);
            probe.put(Probe.LAT, Double.parseDouble(tuple[1]));
            probe.put(Probe.LON, Double.parseDouble(tuple[2]));
            probe.put(Probe.TS, tuple[3]);

            sendMessage(tuple[0], probe);
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
            log.info(line);

            GenericRecord probe = new GenericData.Record(loadAvroSchema());
            probe.put(Probe.KEY, tuple[0]);
            probe.put(Probe.LAT, Double.parseDouble(tuple[1]));
            probe.put(Probe.LON, Double.parseDouble(tuple[2]));
            probe.put(Probe.TS, tuple[3]);

            sendMessage(tuple[0], probe);
        }
    }

    public Schema loadAvroSchema() throws IOException {
        // avro schema avsc file path.
        String schemaPath = "/avro/Probe.avsc";
        // avsc json string.
        String schemaString = null;

        InputStream inputStream = this.getClass().getResourceAsStream(schemaPath);
        try {
            schemaString = IOUtils.toString(inputStream, "UTF-8");
        } finally {
            inputStream.close();
        }
        // avro schema.
        return new Schema.Parser().parse(schemaString);
    }


    public static void main(String [] args){
        Options options = new Options();

        Option isOnK8s = new Option("k", "onK8S", true, "on K8S mode");
        isOnK8s.setRequired(false);
        options.addOption(isOnK8s);

        CommandLineParser parser = new DefaultParser();

        InputStream is;
        BufferedReader br = null;

        try {
            CommandLine cmd = parser.parse(options, args);
            AvroGPSProducer producer;
            if (cmd.getOptionValue("onK8S") == null) {
                producer = new AvroGPSProducer("probe-avro2", false, false);
                producer.log.info("reading from local");
                producer.readFromLocal();
            } else {
                producer = new AvroGPSProducer("probe-avro2", false, true);
//                producer.log.info("reading from S3");
//                producer.readFromS3(BUCKET, BUCKET_KEY);
                producer.log.info("reading from local");
                producer.readFromLocal();
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

