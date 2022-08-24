package ai.lzy.portal.s3;

import com.azure.storage.blob.BlobServiceClient;
import org.apache.commons.lang3.StringUtils;
import ru.yandex.qe.s3.repository.BiDirectS3Converter;
import ru.yandex.qe.s3.transfer.Transmitter;

import java.util.concurrent.ExecutorService;

public class AzureRepositoryAdapter<T> extends ru.yandex.qe.s3.repository.S3Repository<T> implements S3Repository<T> {
    private static final String BUCKET_KEY_DELIMITER = "#";
    private final BlobServiceClient client;

    public AzureRepositoryAdapter(BlobServiceClient client, Transmitter transmitter,
                                  int toStreamPoolSize, BiDirectS3Converter<T> converter) {
        super(transmitter, toStreamPoolSize, "", converter);
        this.client = client;
    }

    public AzureRepositoryAdapter(BlobServiceClient client, Transmitter transmitter,
                                  ExecutorService consumerExecutor,
                                  BiDirectS3Converter<T> converter) {
        super(transmitter, consumerExecutor, "", 0, null, converter);
        this.client = client;
    }

    private static String internalKey(String bucket, String key) {
        return bucket + BUCKET_KEY_DELIMITER + key;
    }

    @Override
    public void put(String bucket, String key, T value) {
        this.put(internalKey(bucket, key), value);
    }

    @Override
    public T get(String bucket, String key) {
        return this.get(internalKey(bucket, key));
    }

    @Override
    public boolean contains(String bucket, String key) {
        return client.getBlobContainerClient(bucket).getBlobClient(internalKey(bucket, key)).exists();
    }

    @Override
    public void remove(String bucket, String key) {
        this.remove(internalKey(bucket, key));
    }

    @Override
    public void createBucket(String bucket) {
        if (!client.getBlobContainerClient(bucket).exists()) {
            client.getBlobContainerClient(bucket).create();
        }
    }

    @Override
    public void deleteObject(String bucket, String key) {
        client.getBlobContainerClient(bucket).getBlobClient(internalKey(bucket, key)).delete();
    }

    @Override
    protected String selectBucket(String keyPrefixedWithBucketName) {
        return StringUtils.substringBefore(keyPrefixedWithBucketName, BUCKET_KEY_DELIMITER);
    }
}
