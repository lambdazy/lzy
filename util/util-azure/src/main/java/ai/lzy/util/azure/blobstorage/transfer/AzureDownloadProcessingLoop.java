package ai.lzy.util.azure.blobstorage.transfer;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Uninterruptibles;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.lang.String.format;

public class AzureDownloadProcessingLoop<T> extends DownloadProcessingLoop<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AzureDownloadProcessingLoop.class);

    private final BlobServiceClient client;
    private final DownloadRetryOptions retryOptions;

    public AzureDownloadProcessingLoop(
        BlobServiceClient client, DownloadRetryOptions retryOptions,
        @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService taskExecutor,
        @Nonnull ListeningExecutorService consumeExecutor, @Nonnull DownloadRequest request,
        @Nonnull Function<MetaAndStream, T> processor, @Nullable Consumer<DownloadState> progressListener,
        @Nullable Executor notifyExecutor
    ) {
        super(byteBufferPool, taskExecutor, consumeExecutor, request, processor, progressListener, notifyExecutor);
        this.client = client;
        this.retryOptions = retryOptions;
    }

    @Override
    protected void consumeContent(String bucket, String key, long rangeStart, long rangeEnd, int partNumber,
        ThrowingConsumer<InputStream> consumer) {
        BlobClient blob = client.getBlobContainerClient(bucket).getBlobClient(key);
        BlobRange range = new BlobRange(rangeStart, rangeEnd - rangeStart);
        int attempt = 1;
        Exception exception = null;
        while (true) {
            if (exception != null) {
                Uninterruptibles.sleepUninterruptibly(delayBeforeNextRetry(exception, attempt), TimeUnit.MILLISECONDS);
            }
            try (InputStream in = blob.openInputStream(range, null)) {
                consumer.acceptThrows(in);
                break;
            } catch (Exception ex) {
                exception = ex;
                LOG.warn("{} during download part {} size {} bytes for {}:{}, attempt {}",
                    category(ex), partNumber, rangeEnd - rangeStart, bucket, key, attempt);
                attempt++;
                if (!shouldRetry(exception, attempt)) {
                    throw new S3TransferException(
                        format("%s during download part %s size %s bytes for %s:%s, was %s retries",
                            category(ex), partNumber, rangeEnd - rangeStart, bucket, key, attempt), ex);
                }
            }
        }
    }

    @Override
    protected Metadata getMetadata(String bucket, String key) {
        BlobProperties props =
            client.getBlobContainerClient(bucket).getBlobClient(key).getBlockBlobClient().getProperties();
        Map<String, Object> meta = new HashMap<>(props.getMetadata());
        return new Metadata(meta, null, null, props.getBlobSize());
    }

    @Override
    protected String errorLogDetails(Throwable throwable) {
        return throwable.toString();
    }

    @Nonnull
    private String category(@Nonnull Exception ex) {
        return ex instanceof IOException ? "i/o" : ex.getClass().getSimpleName();
    }

    private long delayBeforeNextRetry(Exception exception, int retriesAttempted) {
        // TODO better retry policy
        return 1L << retriesAttempted;
    }

    private boolean shouldRetry(Exception exception, int retriesAttempted) {
        return retriesAttempted <= retryOptions.getMaxRetryRequests();
    }
}
