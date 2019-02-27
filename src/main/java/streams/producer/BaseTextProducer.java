package streams.producer;

import java.io.BufferedReader;
import java.io.IOException;

public abstract class BaseTextProducer<T> extends AbstractProducer<T> {

    static BufferedReader br = null;
    protected static final String fileName = "test_trajectories.csv";
    protected static final String BUCKET = "aws-acc-001-1053-r1-master-data-science";
    protected static final String BUCKET_KEY = "test_trajectories.csv";

    public BaseTextProducer(String topic, Boolean isAsync, Boolean onK8s, SERIALIZER serializer) {
        super(topic, isAsync, onK8s, serializer);
    }

    public abstract void readFromS3(String bucketName, String key) throws IOException;

    public abstract void readFromLocal() throws IOException;


}
