package streams.config;

public class ConfigVars {

    //kafka
    public static final String KAFKA_HOST = "KAFKA_HOST";
    public static final String SCHEMA_REGISTRY_HOST = "SCHEMA_REGISTRY_HOST";

    //kafka-avro
    public static final String AVRO_TOPIC_GPS_PRODUCER = "probe-avro";
    public static final String AVRO_TOPIC_GPS_AGG = "gps-trace-output";
    public static final String AVRO_PRODUCER_SCHEMA_PATH = "/avro/Probe.avsc";
    public static final String AVRO_AGG_SCHEMA_PATH = "";

    //kafka-string
    public static final String STRG_TOPIC_GPS_PRODUCER = "gps-topic";
    public static final String STRG_TOPIC_GPS_AGG = "gps-trace-output";

    //kafka-consumer-map-matching
    public static final String CONSUMER_GROUP = "gps-group";
    public static final String MAPMATCH_TOPIC = "mm-topic";

    //aws-s3
    public static final String AWS_ACCESS_KEY = "aws_access_key_id";
    public static final String AWS_SECRET_KEY = "aws_secret_access_key";

}
