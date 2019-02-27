package streams.producer;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

public class BaseProducer extends AbstractProducer<String> {
    public BaseProducer(String topic, Boolean isAsync, Boolean onK8s, SERIALIZER serializer) {
        super(topic, isAsync, onK8s, serializer);
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
