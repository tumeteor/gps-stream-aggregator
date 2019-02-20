package stringser.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class BaseProducer extends Thread {
    protected KafkaProducer<String, String> producer;
    protected static Boolean isAsync;
    protected static String topicName;

    public enum SERIALIZER {
        JSON, STRINGSE
    }

    SERIALIZER serializer;

    private Logger log = LoggerFactory.getLogger(getClass());

    public BaseProducer(String topic, Boolean isAsync, Boolean onK8s, SERIALIZER serializer) {
        Properties props = new Properties();
        this.topicName = topic;

        if (onK8s) {
            //namespace-kafka on k8s
            props.put("bootstrap.servers", System.getenv("KAFKA_HOST"));
        } else {
            props.put("bootstrap.servers", "localhost:9092");
        }
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        this.serializer = serializer;
        switch (this.serializer) {
            case STRINGSE:
                props.put("value.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer");
                break;
            case JSON:
                props.put("value.serializer",
                        "org.apache.kafka.connect.json.JsonSerializer");
                break;
        }
        props.put("kafka.topic"      , topic);
        producer = new KafkaProducer<>(props);
        this.isAsync = isAsync;
    }

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

