package streams.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class AbstractProducer<T> extends Thread {
    protected KafkaProducer producer;
    protected static Boolean isAsync;
    protected static String topicName;

    public enum SERIALIZER {
        JSONSE, STRINGSE, AVROSE
    }

    SERIALIZER serializer;

    protected Logger log = LoggerFactory.getLogger(getClass());

    public AbstractProducer(String topic, Boolean isAsync, Boolean onK8s, SERIALIZER serializer) {
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
            case JSONSE:
                props.put("value.serializer",
                        "org.apache.kafka.connect.json.JsonSerializer");
                break;
            case AVROSE:
                log.info("setting serializer to props");
                props.put("key.serializer",
                        KafkaAvroSerializer.class.getName());
                props.put("value.serializer",
                        KafkaAvroSerializer.class.getName());
                props.put("schema.registry.url", System.getenv("SCHEMA_REGISTRY_HOST"));
//                props.put("schema.registry.url", "http://localhost:18081");
                break;

        }
        props.put("kafka.topic"      , topic);
        producer = new KafkaProducer<>(props);
        log.info("created producer");
        this.isAsync = isAsync;
    }

    abstract void sendMessage(String key, T value);


}

