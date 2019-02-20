package connect_client;

import org.sourcelab.kafka.connect.apiclient.Configuration;
import org.sourcelab.kafka.connect.apiclient.KafkaConnectClient;
import org.sourcelab.kafka.connect.apiclient.request.dto.ConnectorDefinition;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;

public class Main {


    public static void main (String[] args) {
        /*
         * Create a new configuration object.
         *
         * This configuration also allows you to define some optional details on your connection,
         * such as using an outbound proxy (authenticated or not), SSL client settings, etc..
         */
        final Configuration configuration = (System.getenv("KAFKA_CONNECT_HOST") != null)?
                new Configuration(System.getenv("KAFKA_CONNECT_HOST"))
                : new Configuration("localhost:8083");

        /*
         * Create an instance of KafkaConnectClient, passing your configuration.
         */
        final KafkaConnectClient client = new KafkaConnectClient(configuration);


        /*
         * Connector for raw GPS
         */
        final ConnectorDefinition gps_connector = client.addConnector(NewConnectorDefinition.newBuilder()
                .withName("GPSConnector")
                .withConfig("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector")
                .withConfig("tasks.max", 3)
                .withConfig("topics", "gps-topic")
                .withConfig("file", "test.sink.raw")
                .build()
        );

        /*
         * Connector for map-matched results
         */
        final ConnectorDefinition mm_connector = client.addConnector(NewConnectorDefinition.newBuilder()
                .withName("GPSConnector")
                .withConfig("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector")
                .withConfig("tasks.max", 3)
                .withConfig("topics", "mm-topic")
                .withConfig("file", "test.sink.mm")
                .build()
        );

//        client.deleteConnector("GPSConnector");

    }
}
