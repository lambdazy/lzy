package ru.yandex.qe.s3.transfer.factories;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.annotation.Nonnull;
import org.joda.time.Duration;
import ru.yandex.qe.s3.transfer.TTLTransmitter;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;
import ru.yandex.qe.s3.transfer.ttl.TTLUploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadState;
import ru.yandex.qe.s3.ttl.S3Type;
import ru.yandex.qe.s3.ttl.TTLRegister;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Established by terry on 30.01.16.
 */
public abstract class BaseTTLTransmitter extends BaseTransmitter implements TTLTransmitter {

    private final TTLRegister ttlRegister;
    private final Duration defaultTTL;

    public BaseTTLTransmitter(@Nonnull TTLRegister ttlRegister, @Nonnull Duration defaultTTL,
        @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService transferExecutor,
        @Nonnull ListeningExecutorService chunksExecutor, @Nonnull ListeningExecutorService consumeExecutor) {
        super(byteBufferPool, transferExecutor, chunksExecutor, consumeExecutor);
        this.ttlRegister = ttlRegister;
        this.defaultTTL = defaultTTL;
    }

    @Nonnull
    public abstract String getS3Endpoint();

    @Nonnull
    public abstract S3Type geS3Type();

    @Override
    public ListenableFuture<UploadState> upload(@Nonnull UploadRequest request) {
        Duration ttl = defaultTTL;
        if (request instanceof TTLUploadRequest) {
            ttl = ((TTLUploadRequest) request).getTTL();
        }
        ttlRegister.add(geS3Type().name(), getS3Endpoint(), request.getBucket(), request.getKey(),
            ttl.getStandardSeconds());
        return super.upload(request);
    }

    @Override
    public ListenableFuture<UploadState> upload(@Nonnull TTLUploadRequest request) {
        ttlRegister.add(geS3Type().name(), getS3Endpoint(), request.getBucket(), request.getKey(),
            request.getTTL().getStandardSeconds());
        return super.upload(request);
    }

    @Override
    public ListenableFuture<UploadState> upload(@Nonnull TTLUploadRequest request,
        @Nonnull Consumer<UploadState> progressListener, @Nonnull Executor notifyExecutor) {
        ttlRegister.add(geS3Type().name(), getS3Endpoint(), request.getBucket(), request.getKey(),
            request.getTTL().getStandardSeconds());
        return super.upload(request, progressListener, notifyExecutor);
    }
}
