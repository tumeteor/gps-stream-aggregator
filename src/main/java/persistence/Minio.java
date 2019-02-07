package persistence;

import io.minio.MinioClient;
import io.minio.errors.MinioException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Minio {

    private MinioClient minioClient;
    private static Logger log = LoggerFactory.getLogger("Minio");


    public Minio() {

        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");

        try {
            this.minioClient = new MinioClient(System.getenv("MINIO_ENDPOINT"), accessKey, secretKey);
        }
        catch (MinioException e) {
            log.error("Error occurred: " + e);
        }
    }

    public void make_bucket(String bucketName) throws MinioException, XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        boolean isExist = minioClient.bucketExists(bucketName);
        if (isExist) {
            log.error("Bucket already exists.");
        }
        else minioClient.makeBucket(bucketName);
    }

    public void write_to_s3(String bucketName, String objectName, JSONObject json) throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try {
            minioClient.listBuckets().forEach(b -> System.out.println(b.name()));

            Path tempFile = Files.createTempFile(new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()), ".tmp");
            try (InputStream in = new ByteArrayInputStream(json.toString().getBytes());) {
                Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
            }

            minioClient.putObject(bucketName, objectName, tempFile.toString());
            Files.delete(tempFile);
        }
        catch (MinioException e) {
            log.error("Error occurred: " + e);
        }

    }

}
