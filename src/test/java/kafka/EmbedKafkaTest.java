package kafka;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streams.producer.KafkaGPSProducer;

import java.io.IOException;

public class EmbedKafkaTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbedKafkaTest.class);

    @Test
    public void testProducer() throws IOException {
        KafkaGPSProducer producer = new KafkaGPSProducer("gps-test", false, false);




    }

}
