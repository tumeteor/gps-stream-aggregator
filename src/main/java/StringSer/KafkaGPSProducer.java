    package StringSer;

    import org.apache.commons.cli.*;
    import org.apache.kafka.clients.producer.Callback;
    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.ProducerRecord;
    import org.apache.kafka.clients.producer.RecordMetadata;

    import java.io.BufferedReader;
    import java.io.IOException;
    import java.io.InputStream;
    import java.io.InputStreamReader;
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

        private final KafkaProducer<String, String> producer;
        private final Boolean isAsync;

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
                    System.out.println("Sent message: (" + key + ", " + value + ")");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
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
                KafkaGPSProducer producer;
                if (cmd.getOptionValue("onK8S") == null) {
                    producer = new KafkaGPSProducer(topicName, false, false);
                } else {
                    producer = new KafkaGPSProducer(topicName, false, true);
                }

                is = ClassLoader.getSystemClassLoader().getResourceAsStream(fileName);

                //Construct BufferedReader from InputStreamReader
                br = new BufferedReader(new InputStreamReader(is));

                while (true) {
                    final String line = br.readLine();

                    String[] tuple = line.split(",");
                    producer.sendMessage(tuple[0], line);
                    //TRICK to get the stream run continuously
                    //reset the stream
                    if (line == null) {
                        is = ClassLoader.getSystemClassLoader().getResourceAsStream(fileName);
                        br = new BufferedReader(new InputStreamReader(is));
                    }
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
                System.out.println("message(" + key + ", " + message
                        + ") sent to partition(" + metadata.partition() + "), "
                        + "offset(" + metadata.offset() + ") in " + elapsedTime
                        + " ms");
            } else {
                exception.printStackTrace();
            }
        }
    }