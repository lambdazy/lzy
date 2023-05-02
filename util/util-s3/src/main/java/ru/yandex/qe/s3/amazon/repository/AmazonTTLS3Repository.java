package ru.yandex.qe.s3.amazon.repository;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.base.Throwables;
import jakarta.annotation.Nonnull;
import org.joda.time.Duration;
import ru.yandex.qe.s3.repository.BiDirectS3Converter;
import ru.yandex.qe.s3.transfer.StreamSuppliers;
import ru.yandex.qe.s3.transfer.TTLTransmitter;
import ru.yandex.qe.s3.transfer.ttl.TTLUploadRequestBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * Established by terry on 21.07.15.
 */
public class AmazonTTLS3Repository<T> extends AmazonS3Repository<T> {

    public AmazonTTLS3Repository(AmazonS3 amazonS3, TTLTransmitter transmitter, int toStreamPoolSize, String bucketName,
        BiDirectS3Converter<T> converter) {
        super(amazonS3, transmitter, toStreamPoolSize, bucketName, converter);
    }

    public AmazonTTLS3Repository(AmazonS3 amazonS3, TTLTransmitter transmitter, int toStreamPoolSize,
        String bucketPrefix, int bucketsCount, BiDirectS3Converter<T> converter) {
        super(amazonS3, transmitter, toStreamPoolSize, bucketPrefix, bucketsCount, converter);
    }

    public AmazonTTLS3Repository(AmazonS3 amazonS3, TTLTransmitter transmitter, int toStreamPoolSize,
        String bucketPrefix, int bucketsCount, Function<String, Integer> keyHashFunction,
        BiDirectS3Converter<T> converter) {
        super(amazonS3, transmitter, toStreamPoolSize, bucketPrefix, bucketsCount, keyHashFunction, converter);
    }

    public AmazonTTLS3Repository(AmazonS3 amazonS3, TTLTransmitter transmitter, ExecutorService consumerExecutor,
        String bucketPrefix, int bucketsCount, Function<String, Integer> keyHashFunction,
        BiDirectS3Converter<T> converter) {
        super(amazonS3, transmitter, consumerExecutor, bucketPrefix, bucketsCount, keyHashFunction, converter);
    }

    public void put(@Nonnull String key, @Nonnull T value, @Nonnull Duration ttl) {
        final String bucket = selectBucket(key);
        try {
            transmitter.upload(new TTLUploadRequestBuilder().ttl(ttl).key(key).bucket(bucket)
                .stream(StreamSuppliers.lazy(outputStream -> converter.toStream(value, outputStream), consumerExecutor,
                    PIPED_CHUNK_SIZE)).build()).get();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }
}
