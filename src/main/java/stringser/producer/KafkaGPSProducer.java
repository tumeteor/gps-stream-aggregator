package stringser.producer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Simulation of a GPS producer that continuously send GPS data
 * of Vehicles in the form of (VehicleID, lon, lat, timestamp).
 * The stream is sent does not strictly (or at all) follow the timestamp in this
 * simulator.
 */

public class KafkaGPSProducer extends BaseProducer {

    public static final String fileName = "test_trajectories.csv";
    private static final String BUCKET = "aws-acc-001-1053-r1-master-data-science";
    private static final String BUCKET_KEY = "test_trajectories.csv";

    static BufferedReader br = null;

    public KafkaGPSProducer(String topic, Boolean isAsync, Boolean onK8s, SERIALIZER serializer) {
        super(topic, isAsync, onK8s, serializer);
    }

    /**
     * Read sample file from S3
     * @param bucketName
     * @param key
     * @throws IOException
     */
    public void readFromS3(String bucketName, String key) throws IOException {
        String accessKey = System.getenv("AWS_ACCEProducerSS_KEY_ID ");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();

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
        System.out.println("Start reading");
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
                producer = new KafkaGPSProducer("gps-topic", false, false, SERIALIZER.STRINGSE);
                producer.readFromLocal();
            } else {
                producer = new KafkaGPSProducer("gps-topic", false, true, SERIALIZER.STRINGSE);
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
}

