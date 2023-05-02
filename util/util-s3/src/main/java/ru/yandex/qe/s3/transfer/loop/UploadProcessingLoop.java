package ru.yandex.qe.s3.transfer.loop;

import com.amazonaws.util.LengthCheckInputStream;
import com.gc.iotools.stream.is.inspection.StatsInputStream;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.commons.io.input.BoundedInputStream;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qe.s3.transfer.TransferStatus;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.meta.MetadataBuilder;
import ru.yandex.qe.s3.transfer.ttl.TTLUploadRequest;
import ru.yandex.qe.s3.transfer.upload.ConcurrencyConflictResolve;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadState;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Stopwatch.createStarted;
import static java.lang.String.format;
import static ru.yandex.qe.s3.util.io.Streams.autoLogStatStream;

/**
 * Established by terry on 22.07.15.
 */
@NotThreadSafe
public abstract class UploadProcessingLoop extends ProcessingLoop<UploadState> {

    private static final Logger LOG = LoggerFactory.getLogger(UploadProcessingLoop.class);

    private final UploadRequest request;

    private final TransferStateListenerSupport<UploadState> notifier;

    public UploadProcessingLoop(@Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService taskExecutor,
        @Nonnull UploadRequest request,
        @Nullable Consumer<UploadState> progressListener, @Nullable Executor notifyExecutor) {
        super(byteBufferPool, taskExecutor, request.getAbortPolicy());
        this.request = request;
        this.notifier = new TransferStateListenerSupport<>(notifyExecutor, progressListener);
    }

    protected abstract void uploadObject(String bucket, String key, Metadata metadata, byte[] buffer, int offset,
        int length, ConcurrencyConflictResolve concurrencyConflictResolve, @Nullable DateTime expirationTime);

    protected abstract void initMultiPartUpload(String bucket, String key, Metadata metadata,
        ConcurrencyConflictResolve concurrencyConflictResolve, @Nullable DateTime expirationTime);

    protected abstract void uploadObjectPart(String bucket, String key, int partNumber, long partSize,
        byte[] buffer, int offset, int length);

    protected abstract void completeUpload(String bucket, String key, Metadata metadata, int partsCount);

    protected abstract void abortUpload(String bucket, String key);

    protected abstract String errorLogDetails(Throwable throwable);

    @Override
    public UploadState callInner() throws Exception {
        notifyListener(TransferStatus.STARTED);

        initStatAndLogUploadStart();
        final Stopwatch stopwatch = createStarted();
        try (InputStream inputStream = boundIfLengthSpecifiedAndLogStatOnClose()) {
            int nextPartNumber = 1;
            while (isRunning()) {
                processCompletedTasks();
                if (request.getMaxConcurrencyLevel() > chunkTasks.size() || request.getMaxConcurrencyLevel() == 0) {
                    final ByteBuffer byteBuffer = tryBorrowBuffer();
                    if (byteBuffer != null) {
                        if (byteBuffer.position() != 0) {
                            LOG.warn("Borrowed a dirty buffer, this should not happen");
                        }
                        final int filled = fill(byteBuffer, inputStream);
                        if (nextPartNumber == 1) {
                            if (byteBuffer.limit() < byteBuffer.capacity()) {
                                if (filled <= 0 && !request.isAllowEmptyStream()) {
                                    throw new IllegalArgumentException(
                                        format("input for %s:%s is empty!", request.getBucket(), request.getKey()));
                                }
                                final UploadTask uploadTask = new UploadTask(byteBuffer, stopwatch);
                                executeNewTask(uploadTask, logChunkFailCallback(uploadTask));
                                break;
                            } else {
                                initUpload();
                            }
                        }
                        if (filled > 0) {
                            final UploadChunkTask uploadChunkTask = new UploadChunkTask(nextPartNumber, byteBuffer);
                            executeNewTask(uploadChunkTask, logChunkFailCallback(uploadChunkTask));
                            nextPartNumber++;
                        } else {
                            break;
                        }
                    }
                } else {
                    Thread.sleep(WAIT_QUANTUM);
                }
            }
            waitAllTasks();
            if (nextPartNumber > 1) {
                completeUpload(stopwatch, nextPartNumber - 1);
            }
            final UploadState uploadState = new UploadState(TransferStatus.DONE, getCurrentStatistic(), request);
            notifyListener(uploadState);
            return uploadState;
        } catch (Throwable e) {
            final InterruptedException ie = interruptedException(e);
            if (ie != null) {
                handleInterrupt(stopwatch);
                throw ie;
            } else {
                handleThrowable(e, stopwatch);
                throw e;
            }
        }
    }

    private void initStatAndLogUploadStart() {
        final long contentLength = request.getObjectMetadata().getObjectContentLength();
        if (contentLength != Metadata.UNDEFINED_LENGTH) {
            LOG.debug("start upload for {}:{} size {} bytes in {} parts", request.getBucket(),
                request.getKey(), contentLength, chunksCount(contentLength));
        } else {
            LOG.debug("start upload for {}:{} size unknown", request.getBucket(), request.getKey());
        }
        initCurrentStatistic(contentLength);
        notifyListener(TransferStatus.IN_PROGRESS);
    }

    private FutureCallback<Void> logChunkFailCallback(ChunkRunnable chunkRunnable) {
        return new FutureCallback<Void>() {
            @Override
            public void onSuccess(@Nullable Void result) {//nothing
            }

            @SuppressWarnings("NullableProblems")
            @Override
            public void onFailure(Throwable throwable) {
                if (throwable instanceof CancellationException) {
                    LOG.info("canceled upload part {} for {}:{}", chunkRunnable.getPartNumber(), request.getBucket(),
                        request.getKey());
                } else {
                    LOG.warn("fail to upload part {} for {}:{}, implementation specific info: {}",
                        chunkRunnable.getPartNumber(), request.getBucket(), request.getKey(),
                        errorLogDetails(throwable), throwable);
                }
            }
        };
    }

    private InputStream boundIfLengthSpecifiedAndLogStatOnClose() {
        final StatsInputStream inputStream = autoLogStatStream(request.getStreamSupplier().get(),
            format("upload input stream for %s:%s", request.getBucket(), request.getKey()));
        final long contentLength = request.getObjectMetadata().getObjectContentLength();
        return contentLength > 0 ? new LengthCheckInputStream(new BoundedInputStream(inputStream, contentLength),
            contentLength, false) : inputStream;
    }

    private void putObject(ByteBuffer byteBuffer, Stopwatch stopwatch) {
        Metadata objectMetadata = request.getObjectMetadata();
        if (objectMetadata.getObjectContentLength() == Metadata.UNDEFINED_LENGTH) {
            objectMetadata = new MetadataBuilder(objectMetadata).setObjectContentLength(byteBuffer.limit()).build();
        }
        uploadObject(request.getBucket(), request.getKey(), objectMetadata, byteBuffer.array(), 0, byteBuffer.limit(),
            request.getConcurrencyConflictResolve(),
            (request instanceof TTLUploadRequest) ? DateTime.now().plus(((TTLUploadRequest) request).getTTL()) : null);
        LOG.debug("upload {} bytes completed for {}:{} in {} ms", byteBuffer.limit(),
            request.getBucket(), request.getKey(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private void notifyListener(TransferStatus transferStatus) {
        notifier.notifyListener(() -> new UploadState(transferStatus, getCurrentStatistic(), request));
    }

    private void notifyListener(UploadState uploadState) {
        notifier.notifyListener(uploadState);
    }

    private void initUpload() {
        initMultiPartUpload(request.getBucket(), request.getKey(), request.getObjectMetadata(),
            request.getConcurrencyConflictResolve(),
            (request instanceof TTLUploadRequest) ? DateTime.now().plus(((TTLUploadRequest) request).getTTL()) : null);
        LOG.debug("multipart upload started for {}:{}", request.getBucket(), request.getKey());
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    private void completeUpload(Stopwatch stopwatch, int partCount) throws ExecutionException, InterruptedException {
        completeUpload(request.getBucket(), request.getKey(), request.getObjectMetadata(), partCount);
        LOG.debug("multipart upload completed for {}:{} in {} ms",
            request.getBucket(), request.getKey(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private void handleInterrupt(Stopwatch stopwatch) {
        final Stopwatch interruptStopwatch = Stopwatch.createStarted();
        abortUpload(TransferStatus.CANCELED, stopwatch);
        LOG.warn("interrupted upload for {}:{}; upload took {} ms, cancellation took {} ms", request.getBucket(),
            request.getKey(),
            stopwatch.elapsed(TimeUnit.MILLISECONDS), interruptStopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
    }

    private void handleThrowable(Throwable throwable, Stopwatch stopwatch) {
        final Stopwatch failureStopwatch = Stopwatch.createStarted();
        abortUpload(TransferStatus.FAILED, stopwatch);
        LOG.error("fail upload for {}:{}: upload took {} ms, handling failure took {} ms", request.getBucket(),
            request.getKey(),
            stopwatch.elapsed(TimeUnit.MILLISECONDS), failureStopwatch.stop().elapsed(TimeUnit.MILLISECONDS),
            throwable);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    private void abortUpload(TransferStatus finalStatus, Stopwatch stopwatch) {
        final Stopwatch abortStopwatch = Stopwatch.createStarted();
        LOG.warn("aborting upload for {}:{}", request.getBucket(), request.getKey());
        if (Thread.interrupted()) {
            LOG.warn("aborting upload of {}:{}: cleared transmitter thread interrupt flag", request.getBucket(),
                request.getKey());
        }

        try {
            abortUpload(request.getBucket(), request.getKey());
        } catch (Exception e) {
            LOG.warn("caught exception while aborting upload for {}:{}; ignored", request.getBucket(), request.getKey(),
                e);
        }
        awaitTermination(stop());

        LOG.warn("aborted upload for {}:{}: upload took {} ms, abort took {}ms", request.getBucket(), request.getKey(),
            stopwatch.elapsed(TimeUnit.MILLISECONDS), abortStopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
        notifyListener(finalStatus);
    }

    private class UploadChunkTask extends ChunkRunnable {

        private final int partNumber;
        private final ByteBuffer byteBuffer;

        private UploadChunkTask(int partNumber, ByteBuffer byteBuffer) {
            this.partNumber = partNumber;
            this.byteBuffer = byteBuffer;
        }

        @Nonnull
        @Override
        public String getName() {
            return String.format("upload part %d of %s:%s", getPartNumber(), request.getBucket(), request.getKey());
        }

        public int getPartNumber() {
            return partNumber;
        }

        @Override
        protected void doRun() {
            final Stopwatch stopwatch = createStarted();

            checkCanceled();
            LOG.debug("start upload part {} size {} for {}:{}", partNumber, byteBuffer.limit(), request.getBucket(),
                request.getKey());
            uploadObjectPart(request.getBucket(), request.getKey(), partNumber, byteBuffer.limit(),
                byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());

            checkCanceled();
            LOG.debug("complete upload part {} size {} bytes for {}:{} in {} ms", partNumber,
                byteBuffer.limit(), request.getBucket(), request.getKey(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            incrementCurrentStatistic();
            notifyListener(TransferStatus.IN_PROGRESS);
        }
    }

    private class UploadTask extends ChunkRunnable {

        private final ByteBuffer byteBuffer;
        private final Stopwatch stopwatch;

        private UploadTask(ByteBuffer byteBuffer, Stopwatch stopwatch) {
            this.byteBuffer = byteBuffer;
            this.stopwatch = stopwatch;
        }

        @Nonnull
        @Override
        public String getName() {
            return String.format("upload %s:%s", request.getBucket(), request.getKey());
        }

        @Override
        public int getPartNumber() {
            return 1;
        }

        @Override
        protected void doRun() {
            checkCanceled();
            putObject(byteBuffer, stopwatch);
            incrementCurrentStatistic();
        }
    }
}
