package ru.yandex.qe.s3.repository;

import com.google.common.base.Throwables;
import jakarta.annotation.Nonnull;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import ru.yandex.qe.s3.transfer.StreamSuppliers;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;
import ru.yandex.qe.s3.transfer.upload.UploadRequestBuilder;

import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * Established by terry on 02.07.15.
 */
public abstract class S3Repository<T> {

    protected static final int PIPED_CHUNK_SIZE = 64 * 1024;
    protected final BiDirectS3Converter<T> converter;
    private final String bucketPrefix;
    private final int bucketsCount;
    private final Function<String, Integer> keyHashFunction;
    protected Transmitter transmitter;
    protected ExecutorService consumerExecutor;

    public S3Repository(Transmitter transmitter, int toStreamPoolSize, String bucketName,
        BiDirectS3Converter<T> converter) {
        this(transmitter, toStreamPoolSize, bucketName, 0, key -> Math.abs(key.hashCode()), converter);
    }

    public S3Repository(Transmitter transmitter, int toStreamPoolSize, String bucketPrefix, int bucketsCount,
        BiDirectS3Converter<T> converter) {
        this(transmitter, toStreamPoolSize, bucketPrefix, bucketsCount, key -> Math.abs(key.hashCode()), converter);
    }

    public S3Repository(Transmitter transmitter, int toStreamPoolSize, String bucketPrefix, int bucketsCount,
        Function<String, Integer> keyHashFunction,
        BiDirectS3Converter<T> converter) {
        this(transmitter, newFixedThreadPool(toStreamPoolSize, new BasicThreadFactory.Builder()
                .namingPattern("s3-repository-producer-%d").daemon(true).priority(Thread.NORM_PRIORITY).build()),
            bucketPrefix, bucketsCount, keyHashFunction, converter
        );
    }

    public S3Repository(Transmitter transmitter, ExecutorService consumerExecutor, String bucketPrefix,
        int bucketsCount, Function<String, Integer> keyHashFunction,
        BiDirectS3Converter<T> converter) {
        this.bucketPrefix = bucketPrefix;
        this.bucketsCount = bucketsCount;
        this.keyHashFunction = keyHashFunction;
        this.converter = converter;
        this.transmitter = transmitter;
        this.consumerExecutor = consumerExecutor;
    }

    public abstract void createBucket(String bucket);

    public abstract void deleteObject(String bucket, String key);

    /**
     * Must be invoke in post construct phase, this method invoke s3 http requests
     */
    public void init() {
        if (bucketsCount == 0) {
            createBucket(bucketPrefix);
        } else {
            for (int i = 0; i < bucketsCount; i++) {
                createBucket(bucketPrefix + i);
            }
        }
    }

    public void put(String bucket, String key, T value) {
        try {
            transmitter.upload(new UploadRequestBuilder().key(key).bucket(bucket)
                .stream(StreamSuppliers.lazy(outputStream -> converter.toStream(value, outputStream), consumerExecutor,
                    PIPED_CHUNK_SIZE)).build()).get();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    public void put(@Nonnull String key, @Nonnull T value) {
        final String bucket = selectBucket(key);
        put(bucket, key, value);
    }

    public T get(String bucket, String key) {
        try {
            return transmitter.downloadF(new DownloadRequestBuilder().bucket(bucket).key(key).build(),
                    metaAndStream -> {
                        try (InputStream inputStream = metaAndStream.getInputStream()) {
                            return converter.fromStream(inputStream);
                        }
                    }).get().getProcessingResult();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    public T get(@Nonnull String key) {
        final String bucket = selectBucket(key);
        return get(bucket, key);
    }

    public void remove(String bucket, String key) {
        deleteObject(bucket, key);
    }

    public void remove(@Nonnull String key) {
        remove(selectBucket(key), key);
    }

    protected String selectBucket(String key) {
        return bucketsCount == 0 ? bucketPrefix : bucketPrefix + keyHashFunction.apply(key) % bucketsCount;
    }
}
