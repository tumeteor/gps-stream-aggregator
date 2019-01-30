package StringSer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaGPSProducer extends Thread {

    private static final String topicName
            = "my-topic";
    public static final String fileName = "test_trajectories.csv";

    private final KafkaProducer<String, String> producer;
    private final Boolean isAsync;

    public KafkaGPSProducer(String topic, Boolean isAsync) {
        System.out.println("kafka host: "+ System.getenv("KAFKA_HOST"));
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv("KAFKA_HOST"));
        //namespace-kafka on k8s
        //props.put("bootstrap.servers", "localhost:9092");

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("kafka.topic"      , topic);
        producer = new KafkaProducer<>(props);
        this.isAsync = isAsync;
    }

    public void sendMessage(String key, String value) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            producer.send(
                    new ProducerRecord<>(topicName, key),
                    (Callback) new DemoCallBack(startTime, key, value));
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
        KafkaGPSProducer producer = new KafkaGPSProducer(topicName, false);

        InputStream is;
        BufferedReader br = null;
        try {
            is = ClassLoader.getSystemClassLoader().getResourceAsStream(fileName);
            //Construct BufferedReader from InputStreamReader
            br = new BufferedReader(new InputStreamReader(is));

            String line = null;
            while ((line = br.readLine()) != null) {
                String[] tuple = line.split(",");
                producer.sendMessage(tuple[0], line);
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                br.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
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