package ai.lzy.servant.portal.s3;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.commons.lang3.StringUtils;
import ru.yandex.qe.s3.amazon.repository.AmazonS3Repository;
import ru.yandex.qe.s3.repository.BiDirectS3Converter;
import ru.yandex.qe.s3.transfer.Transmitter;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class AmazonS3RepositoryAdapter<T> extends AmazonS3Repository<T> implements S3Repository<T> {
    private static final String BUCKET_KEY_DELIMITER = "#";

    public AmazonS3RepositoryAdapter(AmazonS3 amazonS3, Transmitter transmitter, int toStreamPoolSize,
                                     BiDirectS3Converter<T> converter) {
        super(amazonS3, transmitter, toStreamPoolSize, "", converter);
    }

    public AmazonS3RepositoryAdapter(AmazonS3 amazonS3, Transmitter transmitter, int toStreamPoolSize, int bucketsCount,
                                     BiDirectS3Converter<T> converter) {
        super(amazonS3, transmitter, toStreamPoolSize, "", bucketsCount, converter);
    }

    public AmazonS3RepositoryAdapter(AmazonS3 amazonS3, Transmitter transmitter, int toStreamPoolSize, int bucketsCount,
                                     Function<String, Integer> keyHashFunction, BiDirectS3Converter<T> converter) {
        super(amazonS3, transmitter, toStreamPoolSize, "", bucketsCount, keyHashFunction, converter);
    }

    public AmazonS3RepositoryAdapter(AmazonS3 amazonS3, Transmitter transmitter, ExecutorService consumerExecutor,
                                     int bucketsCount, Function<String, Integer> keyHashFunction,
                                     BiDirectS3Converter<T> converter) {
        super(amazonS3, transmitter, consumerExecutor, "", bucketsCount, keyHashFunction, converter);
    }

    private static String internalKey(String bucket, String key) {
        return bucket + BUCKET_KEY_DELIMITER + key;
    }

    @Override
    public void put(@Nonnull String bucket, @Nonnull String key, @Nonnull T value) {
        this.put(internalKey(bucket, key), value);
    }

    @Override
    public T get(@Nonnull String bucket, @Nonnull String key) {
        return this.get(internalKey(bucket, key));
    }

    @Override
    public boolean containsKey(String bucket, String key) {
        return false;
    }

    @Override
    public void remove(@Nonnull String bucket, @Nonnull String key) {
        this.remove(internalKey(bucket, key));
    }

    @Override
    protected String selectBucket(String keyPrefixedWithBucketName) {
        return StringUtils.substringBefore(keyPrefixedWithBucketName, BUCKET_KEY_DELIMITER);
    }
}
