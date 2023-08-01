package ai.lzy.common.storage;

import ai.lzy.common.storage.azure.AzureClientWithTransmitter;
import ai.lzy.common.storage.s3.S3ClientWithTransmitter;
import ai.lzy.v1.common.LMST;
import jakarta.annotation.Nonnull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class StorageClientFactory {
    private final int byteBufferPoolSize;

    private final ExecutorService transferPool;
    private final ExecutorService chunkPool;
    private final ExecutorService consumePool;

    public StorageClientFactory(int downloadsPoolSize, int chunksPoolSize) {
        this.byteBufferPoolSize = downloadsPoolSize + chunksPoolSize;

        this.transferPool = Executors.newFixedThreadPool(downloadsPoolSize, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(@Nonnull Runnable r) {
                return new Thread(r, "transfer-pool-" + counter.getAndIncrement());
            }
        });

        this.chunkPool = Executors.newFixedThreadPool(chunksPoolSize, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(@Nonnull Runnable r) {
                return new Thread(r, "chunk-pool-" + counter.getAndIncrement());
            }
        });

        this.consumePool = Executors.newFixedThreadPool(downloadsPoolSize, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(@Nonnull Runnable r) {
                return new Thread(r, "consume-pool-" + counter.getAndIncrement());
            }
        });
    }

    @SuppressWarnings("unused")
    public void destroy() {
        Consumer<ExecutorService> stop = service -> {
            service.shutdownNow();
            try {
                //noinspection ResultOfMethodCallIgnored
                service.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                // blank
            }
        };

        stop.accept(transferPool);
        stop.accept(chunkPool);
        stop.accept(consumePool);
    }

    record S3Provider(String endpoint, String accessToken, String secretToken, int byteBufferSize,
                      ExecutorService downloadUploadPool, ExecutorService chunkPool, ExecutorService consumePool)
        implements Supplier<S3ClientWithTransmitter>
    {
        @Override
        public S3ClientWithTransmitter get() {
            return new S3ClientWithTransmitter(endpoint, accessToken, secretToken, byteBufferSize, downloadUploadPool,
                chunkPool, consumePool);
        }
    }

    record AzureProvider(String connectionStr, int byteBufferSize, ExecutorService transferPool,
                         ExecutorService chunkPool, ExecutorService consumePool)
        implements Supplier<AzureClientWithTransmitter>
    {
        @Override
        public AzureClientWithTransmitter get() {
            return new AzureClientWithTransmitter(connectionStr, byteBufferSize, transferPool, chunkPool, consumePool);
        }
    }

    public Supplier<? extends StorageClientWithTransmitter> provider(LMST.StorageConfig storageConfig) {
        if (storageConfig.hasAzure()) {
            return new AzureProvider(storageConfig.getAzure().getConnectionString(), byteBufferPoolSize, transferPool,
                chunkPool, consumePool);
        } else {
            assert storageConfig.hasS3();
            return new S3Provider(storageConfig.getS3().getEndpoint(), storageConfig.getS3().getAccessToken(),
                storageConfig.getS3().getSecretToken(), byteBufferPoolSize, transferPool, chunkPool, consumePool);
        }
    }
}
