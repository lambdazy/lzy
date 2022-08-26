package ai.lzy.portal.s3;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.commons.lang3.StringUtils;
import ru.yandex.qe.s3.amazon.repository.AmazonS3Repository;
import ru.yandex.qe.s3.repository.BiDirectS3Converter;
import ru.yandex.qe.s3.transfer.Transmitter;

import java.util.concurrent.ExecutorService;

public class AmazonS3RepositoryAdapter<T> extends AmazonS3Repository<T> implements S3Repository<T> {
    private static final String BUCKET_KEY_DELIMITER = "#";

    public AmazonS3RepositoryAdapter(AmazonS3 amazonS3, Transmitter transmitter,
                                     int toStreamPoolSize, BiDirectS3Converter<T> converter) {
        super(amazonS3, transmitter, toStreamPoolSize, "", converter);
    }

    public AmazonS3RepositoryAdapter(AmazonS3 amazonS3, Transmitter transmitter,
                                     ExecutorService consumerExecutor,
                                     BiDirectS3Converter<T> converter) {
        super(amazonS3, transmitter, consumerExecutor, "", 0, null, converter);
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
        return amazonS3.doesObjectExist(bucket, internalKey(bucket, key));
    }

    @Override
    public void remove(String bucket, String key) {
        this.remove(internalKey(bucket, key));
    }

    @Override
    public void deleteObject(String bucket, String key) {
        super.deleteObject(bucket, internalKey(bucket, key));
    }

    @Override
    protected String selectBucket(String keyPrefixedWithBucketName) {
        return StringUtils.substringBefore(keyPrefixedWithBucketName, BUCKET_KEY_DELIMITER);
    }
}
