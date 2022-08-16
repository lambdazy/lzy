package ai.lzy.servant.portal;

import ai.lzy.servant.portal.s3.AmazonS3RepositoryAdapter;
import ai.lzy.servant.portal.s3.AzureRepositoryAdapter;
import ai.lzy.servant.portal.s3.S3Repository;
import ai.lzy.util.azure.blobstorage.AzureTransmitterFactory;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.azure.storage.blob.BlobServiceClientBuilder;
import ru.yandex.qe.s3.amazon.transfer.AmazonTransmitterFactory;
import ru.yandex.qe.s3.repository.BiDirectS3Converter;
import ru.yandex.qe.s3.transfer.Transmitter;

import java.util.HashMap;
import java.util.Map;

public class S3Repositories<T> {

    public interface S3RepositoryProvider<U> {
        S3Repository<U> getRepository();
    }

    static final String DEFAULT_TRANSMITTER_NAME = "transmitter";
    static final int DEFAULT_DOWNLOAD_POOL_SIZE = 10;
    static final int DEFAULT_UPLOAD_POOL_SIZE = 10;

    private final Map<S3RepositoryProvider<T>, S3Repository<T>> repositories = new HashMap<>();

    public S3Repository<T> getOrCreate(String endpoint, String accessToken, String secretToken,
                                       BiDirectS3Converter<T> converter) {
        return repositories.computeIfAbsent(AmazonS3Key.of(endpoint, accessToken, secretToken, converter),
            S3RepositoryProvider::getRepository);
    }

    public S3Repository<T> getOrCreate(String connectionString, BiDirectS3Converter<T> converter) {
        return repositories.computeIfAbsent(AzureS3Key.of(connectionString, converter),
            S3RepositoryProvider::getRepository);
    }

    record AmazonS3Key<T>(String endpoint, String accessToken, String secretToken, BiDirectS3Converter<T> converter)
        implements S3RepositoryProvider<T> {
        static <U> AmazonS3Key<U> of(String endpoint, String accessToken, String secretToken,
                                     BiDirectS3Converter<U> converter) {
            return new AmazonS3Key<>(endpoint, accessToken, secretToken, converter);
        }

        @Override
        public AmazonS3RepositoryAdapter<T> getRepository() {
            BasicAWSCredentials credentials = new BasicAWSCredentials(accessToken, secretToken);
            AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AmazonS3ClientBuilder.EndpointConfiguration(endpoint, "us-west-1"))
                .withPathStyleAccessEnabled(true)
                .build();
            Transmitter transmitter = new AmazonTransmitterFactory(client).fixedPoolsTransmitter(
                DEFAULT_TRANSMITTER_NAME, DEFAULT_DOWNLOAD_POOL_SIZE, DEFAULT_UPLOAD_POOL_SIZE
            );
            return new AmazonS3RepositoryAdapter<T>(client, transmitter, DEFAULT_DOWNLOAD_POOL_SIZE, converter);
        }
    }

    record AzureS3Key<T>(String connectionString, BiDirectS3Converter<T> converter) implements S3RepositoryProvider<T> {
        static <U> AzureS3Key<U> of(String connectionString, BiDirectS3Converter<U> converter) {
            return new AzureS3Key<>(connectionString, converter);
        }

        @Override
        public AzureRepositoryAdapter<T> getRepository() {
            var client = new BlobServiceClientBuilder().connectionString(connectionString)
                .buildClient();
            var transmitter = new AzureTransmitterFactory(client).fixedPoolsTransmitter(
                DEFAULT_TRANSMITTER_NAME, DEFAULT_DOWNLOAD_POOL_SIZE, DEFAULT_UPLOAD_POOL_SIZE
            );
            return new AzureRepositoryAdapter<T>(client, transmitter, DEFAULT_DOWNLOAD_POOL_SIZE, converter);
        }
    }
}
