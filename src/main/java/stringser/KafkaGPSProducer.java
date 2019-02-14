    package stringser;

    import com.amazonaws.auth.AWSCredentials;
    import com.amazonaws.auth.AWSStaticCredentialsProvider;
    import com.amazonaws.auth.BasicAWSCredentials;
    import com.amazonaws.services.s3.AmazonS3;
    import com.amazonaws.services.s3.AmazonS3ClientBuilder;
    import com.amazonaws.services.s3.model.GetObjectRequest;
    import com.amazonaws.services.s3.model.S3Object;
    import org.apache.commons.cli.*;
    import org.apache.kafka.clients.producer.Callback;
    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.ProducerRecord;
    import org.apache.kafka.clients.producer.RecordMetadata;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.io.*;
    import java.util.Properties;
    import java.util.concurrent.ExecutionException;


    /**
     * Simulation of a GPS producer that continuously send GPS data
     * of Vehicles in the form of (VehicleID, lon, lat, timestamp).
     * The stream is sent does not strictly (or at all) follow the timestamp in this
     * simulator.
     */
    public class KafkaGPSProducer extends Thread {

        private static final String topicName
                = "gps-topic";
        public static final String fileName = "test_trajectories.csv";
        private static final String BUCKET = "aws-acc-001-1053-r1-master-data-science";
        private static final String BUCKET_KEY = "test_trajectories.csv";

        private final KafkaProducer<String, String> producer;
        private final Boolean isAsync;
        static BufferedReader br = null;

        private static Logger log = LoggerFactory.getLogger("KafkaGPSProducer");


        /*
        Switch mode for K8S or docker-compose
         */
        private final Boolean onK8s;

        public KafkaGPSProducer(String topic, Boolean isAsync, Boolean onK8s) {
            Properties props = new Properties();

            if (onK8s) {
                //namespace-kafka on k8s
                props.put("bootstrap.servers", System.getenv("KAFKA_HOST"));
            } else {
                props.put("bootstrap.servers", "localhost:9092");
            }

            props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");

            props.put("kafka.topic"      , topic);
            producer = new KafkaProducer<>(props);
            this.isAsync = isAsync;
            this.onK8s = onK8s;
        }

        public void sendMessage(String key, String value) {
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(
                        new ProducerRecord<>(topicName, key),
                        new DemoCallBack(startTime, key, value));
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

        /**
         * Read sample file from S3
         * @param bucketName
         * @param key
         * @throws IOException
         */
        public void readFromS3(String bucketName, String key) throws IOException {
            String accessKey = System.getenv("AWS_ACCESS_KEY_ID ");
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
                    producer = new KafkaGPSProducer(topicName, false, false);
                    producer.readFromLocal();
                } else {
                    producer = new KafkaGPSProducer(topicName, false, true);
                    //producer.readFromS3(BUCKET, BUCKET_KEY);
                    producer.readFromLocal();
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

    class DemoCallBack implements Callback {

        private long startTime;
        private String key;
        private String message;
        private static Logger log = LoggerFactory.getLogger("DemoCallBack");

        public DemoCallBack(long startTime, String key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling
         * of request completion. This method will be called when the record sent to
         * the server has been acknowledged. Exactly one of the arguments will be
         * non-null.
         *
         * @param metadata
         *            The metadata for the record that was sent (i.e. the partition
         *            and offset). Null if an error occurred.
         * @param exception
         *            The exception thrown during processing of this record. Null if
         *            no error occurred.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                log.info("message(" + key + ", " + message
                        + ") sent to partition(" + metadata.partition() + "), "
                        + "offset(" + metadata.offset() + ") in " + elapsedTime
                        + " ms");
            } else {
                exception.printStackTrace();
            }
        }
    }