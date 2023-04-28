package ru.yandex.qe.s3.amazon.transfer.loop;

import com.amazonaws.AmazonClientException;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Uninterruptibles;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qe.s3.exceptions.S3TransferException;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.download.DownloadState;
import ru.yandex.qe.s3.transfer.download.MetaAndStream;
import ru.yandex.qe.s3.transfer.loop.DownloadProcessingLoop;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.util.function.ThrowingConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

import static java.lang.String.format;

/**
 * Established by terry on 18.01.16.
 */
@NotThreadSafe
public class AmazonDownloadProcessingLoop<T> extends DownloadProcessingLoop<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonDownloadProcessingLoop.class);

    private final AmazonS3 amazonS3;
    private final RetryPolicy streamReadFailRetryPolicy;

    public AmazonDownloadProcessingLoop(AmazonS3 amazonS3, RetryPolicy streamReadFailRetryPolicy,
        ByteBufferPool byteBufferPool,
        ListeningExecutorService taskExecutor, ListeningExecutorService consumeExecutor, DownloadRequest request,
        Function<MetaAndStream, T> processor,
        Consumer<DownloadState> progressListener, Executor notifyExecutor) {
        super(byteBufferPool, taskExecutor, consumeExecutor, request, processor, progressListener, notifyExecutor);
        this.amazonS3 = amazonS3;
        this.streamReadFailRetryPolicy = streamReadFailRetryPolicy;
    }

    @Override
    protected void consumeContent(String bucket, String key, long rangeStart, long rangeEnd, int partNumber,
        ThrowingConsumer<InputStream> consumer) {
        final GetObjectRequest request = new GetObjectRequest(bucket, key)
            .withRange(rangeStart, rangeEnd - 1);

        int attempt = 1;
        Exception exception = null;
        while (true) {
            if (exception != null) {
                Uninterruptibles.sleepUninterruptibly(delayBeforeNextRetry(request, exception, attempt),
                    TimeUnit.MILLISECONDS);
            }
            try (InputStream chunkStream = amazonS3.getObject(request).getObjectContent()) {
                consumer.acceptThrows(chunkStream);
                break;
            } catch (Exception ex) {
                exception = ex;
                LOG.warn("{} during download part {} size {} bytes for {}:{}, attempt {}",
                    category(ex), partNumber, rangeEnd - rangeStart, bucket, request.getKey(), attempt);
                attempt++;
                if (!shouldRetry(request, exception, attempt)) {
                    throw new S3TransferException(
                        format("%s during download part %s size %s bytes for %s:%s, was %s retries",
                            category(ex), partNumber, rangeEnd - rangeStart, bucket, request.getKey(), attempt), ex);
                }
            }
        }
    }

    @Nonnull
    private String category(@Nonnull Exception ex) {
        return ex instanceof IOException ? "i/o" : ex.getClass().getSimpleName();
    }

    private boolean shouldRetry(GetObjectRequest request, Exception exception, int retriesAttempted) {
        return retriesAttempted <= streamReadFailRetryPolicy.getMaxErrorRetry()
            && streamReadFailRetryPolicy.getRetryCondition()
                .shouldRetry(request, new AmazonClientException(exception), retriesAttempted);
    }

    private long delayBeforeNextRetry(GetObjectRequest request, Exception exception, int retriesAttempted) {
        return streamReadFailRetryPolicy.getBackoffStrategy()
            .delayBeforeNextRetry(request, new AmazonClientException(exception), retriesAttempted);
    }

    @Override
    protected Metadata getMetadata(String bucket, String key) {
        return AmazonMetadataConverter.to(amazonS3.getObjectMetadata(bucket, key));
    }

    @Override
    protected String errorLogDetails(Throwable throwable) {
        return ErrorLogUtils.errorLogDetails(throwable);
    }
}
