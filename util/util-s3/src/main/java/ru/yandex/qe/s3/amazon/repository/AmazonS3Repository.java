package ru.yandex.qe.s3.amazon.repository;

import static java.util.concurrent.Executors.newFixedThreadPool;

import com.amazonaws.services.s3.AmazonS3;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import ru.yandex.qe.s3.repository.S3Repository;
import ru.yandex.qe.s3.transfer.Transmitter;

/**
 * Established by terry on 02.07.15.
 */
public class AmazonS3Repository<T> extends S3Repository<T> {

    private final AmazonS3 amazonS3;

    public AmazonS3Repository(AmazonS3 amazonS3, Transmitter transmitter, int toStreamPoolSize, String bucketName,
        ru.yandex.qe.s3.repository.BiDirectS3Converter<T> converter) {
        this(amazonS3, transmitter, toStreamPoolSize, bucketName, 0, key -> Math.abs(key.hashCode()), converter);
    }

    public AmazonS3Repository(AmazonS3 amazonS3, Transmitter transmitter, int toStreamPoolSize, String bucketPrefix,
        int bucketsCount, ru.yandex.qe.s3.repository.BiDirectS3Converter<T> converter) {
        this(amazonS3, transmitter, toStreamPoolSize, bucketPrefix, bucketsCount, key -> Math.abs(key.hashCode()),
            converter);
    }

    public AmazonS3Repository(AmazonS3 amazonS3, Transmitter transmitter, int toStreamPoolSize, String bucketPrefix,
        int bucketsCount, Function<String, Integer> keyHashFunction,
        ru.yandex.qe.s3.repository.BiDirectS3Converter<T> converter) {
        this(amazonS3, transmitter, newFixedThreadPool(toStreamPoolSize, new BasicThreadFactory.Builder()
                .namingPattern("s3-repository-producer-%d").daemon(true).priority(Thread.NORM_PRIORITY).build()),
            bucketPrefix, bucketsCount, keyHashFunction, converter
        );
    }

    public AmazonS3Repository(AmazonS3 amazonS3, Transmitter transmitter, ExecutorService consumerExecutor,
        String bucketPrefix, int bucketsCount, Function<String, Integer> keyHashFunction,
        ru.yandex.qe.s3.repository.BiDirectS3Converter<T> converter) {
        super(transmitter, consumerExecutor, bucketPrefix, bucketsCount, keyHashFunction, converter);
        this.amazonS3 = amazonS3;
    }


    @Override
    public void createBucket(String bucket) {
        if (!amazonS3.doesBucketExist(bucket)) {
            amazonS3.createBucket(bucket);
        }
    }

    @Override
    public void deleteObject(String bucket, String key) {
        amazonS3.deleteObject(bucket, key);
    }
}
