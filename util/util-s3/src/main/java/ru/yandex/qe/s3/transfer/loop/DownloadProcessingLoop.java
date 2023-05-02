package ru.yandex.qe.s3.transfer.loop;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Uninterruptibles;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qe.s3.transfer.TransferStatus;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.download.DownloadResult;
import ru.yandex.qe.s3.transfer.download.DownloadState;
import ru.yandex.qe.s3.transfer.download.MetaAndStream;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.util.function.ThrowingConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Stopwatch.createStarted;
import static java.lang.String.format;
import static ru.yandex.qe.s3.transfer.download.DownloadRequest.UNDEFF_BOUND_VALUE;
import static ru.yandex.qe.s3.util.io.Streams.autoLogStatStream;

/**
 * Established by terry on 22.07.15.
 */
@NotThreadSafe
public abstract class DownloadProcessingLoop<T> extends ProcessingLoop<DownloadResult<T>> {

    private static final Logger LOG = LoggerFactory.getLogger(DownloadProcessingLoop.class);

    private static final int PIPED_CHUNK_SIZE = 64 * 1024;

    private final DownloadRequest request;

    private final TransferStateListenerSupport<DownloadState> notifier;

    private final Function<MetaAndStream, T> processor;
    private final ListeningExecutorService consumeExecutor;

    private Metadata metadata;
    private long start;
    private long end;
    private long length;

    private OutputStream producingStream;
    private ListenableFuture<T> consumerFuture;

    public DownloadProcessingLoop(@Nonnull ByteBufferPool byteBufferPool,
        @Nonnull ListeningExecutorService taskExecutor,
        @Nonnull ListeningExecutorService consumeExecutor, @Nonnull DownloadRequest request,
        @Nonnull Function<MetaAndStream, T> processor,
        @Nullable Consumer<DownloadState> progressListener, @Nullable Executor notifyExecutor) {
        super(byteBufferPool, taskExecutor, request.getAbortPolicy());
        this.consumeExecutor = consumeExecutor;
        this.request = request;
        this.processor = processor;
        this.notifier = new TransferStateListenerSupport<>(notifyExecutor, progressListener);
    }

    protected abstract void consumeContent(String bucket, String key, long rangeStart, long rangeEnd, int partNumber,
        ThrowingConsumer<InputStream> consumer);

    protected abstract Metadata getMetadata(String bucket, String key);

    protected abstract String errorLogDetails(Throwable throwable);

    @Override
    protected DownloadResult<T> callInner() throws Exception {
        notifyListener(TransferStatus.STARTED);

        final Stopwatch stopwatch = createStarted();

        try {
            getMetadataAndInitDownload();
            initStatAndLogDownloadStart();

            final PipedOutputStream pipedOutputStream = new PipedOutputStream();
            final PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream, PIPED_CHUNK_SIZE);
            try (OutputStream ignored = this.producingStream = autoLogStatStream(pipedOutputStream,
                format("download for %s:%s", request.getBucket(), request.getKey()))) {
                consumerFuture = consumeExecutor.submit(() -> processor.apply(new MetaAndStream(metadata,
                    autoLogStatStream(pipedInputStream,
                        format("download consumer for %s:%s", request.getBucket(), request.getKey())))));

                final long toDownloadChunksCount = chunksCount(length);
                int nextPartNumber = 1;
                while (isRunning() && nextPartNumber <= toDownloadChunksCount) {
                    checkConsumer();
                    processCompletedTasks();
                    if (request.getMaxConcurrencyLevel() > chunkTasks.size() || request.getMaxConcurrencyLevel() == 0) {
                        final ByteBuffer byteBuffer = tryBorrowBuffer();
                        if (byteBuffer != null) {
                            final DownloadChunkTask downloadChunkTask =
                                new DownloadChunkTask(nextPartNumber, byteBuffer);
                            executeNewTask(downloadChunkTask, logChunkFailCallback(downloadChunkTask));
                            nextPartNumber++;
                        }
                    } else {
                        Thread.sleep(WAIT_QUANTUM);
                    }
                }
                waitAllTasks(this::checkConsumer);
            }
            final T processingResult;
            try {
                processingResult = consumerFuture.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                throw new ConsumerException(
                    format("the consumer for %s:%s failed: %s", request.getBucket(), request.getKey(), cause),
                    cause
                );
            }
            final DownloadState downloadState =
                new DownloadState(TransferStatus.DONE, getCurrentStatistic(), request, metadata);
            notifyListener(downloadState);
            return new DownloadResult<>(downloadState, processingResult);
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

    private void notifyListener(TransferStatus transferStatus) {
        notifier.notifyListener(() -> new DownloadState(transferStatus, getCurrentStatistic(), request, metadata));
    }

    private void notifyListener(DownloadState downloadState) {
        notifier.notifyListener(downloadState);
    }

    private void checkConsumer() {
        if (consumerFuture.isDone() || consumerFuture.isCancelled()) {
            try {
                consumerFuture.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                throw new ConsumerException(
                    format("the consumer failed without reading the whole data stream %s:%s %s", request.getBucket(),
                        request.getKey(), cause), cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted: " + e.toString(), e);
            }
            throw new ConsumerException(
                format("the consumer is left without reading the whole data stream %s:%s", request.getBucket(),
                    request.getKey()));
        }
    }

    private void initStatAndLogDownloadStart() {
        final long contentLength = metadata.getObjectContentLength();
        LOG.debug("start download for {}:{} size {} bytes in {} parts", request.getBucket(),
            request.getKey(), contentLength, chunksCount(contentLength));
        initCurrentStatistic(contentLength);
        notifyListener(TransferStatus.IN_PROGRESS);
    }

    private void getMetadataAndInitDownload() {
        metadata = getMetadata(request.getBucket(), request.getKey());

        start = request.getStart() != UNDEFF_BOUND_VALUE ? request.getStart() : 0;
        end = request.getEnd() != UNDEFF_BOUND_VALUE ? request.getEnd() : metadata.getObjectContentLength();
        length = end - start;
    }

    private FutureCallback<Void> logChunkFailCallback(DownloadChunkTask chunkTask) {
        return new FutureCallback<Void>() {
            @Override
            public void onSuccess(@Nullable Void result) {
            }

            @SuppressWarnings("NullableProblems")
            @Override
            public void onFailure(Throwable throwable) {
                if (throwable instanceof CancellationException) {
                    LOG.debug("canceled download part {} for {}:{}", chunkTask.getPartNumber(), request.getBucket(),
                        request.getKey());
                } else {
                    LOG.warn("fail to download part {} for {}:{}, implementation specific info: {}",
                        chunkTask.getPartNumber(), request.getBucket(), request.getKey(),
                        errorLogDetails(throwable), throwable);
                }
            }
        };
    }

    private void handleInterrupt(Stopwatch stopwatch) {
        final Stopwatch interruptStopwatch = Stopwatch.createStarted();
        abortDownload(TransferStatus.CANCELED, stopwatch);
        LOG.error("interrupted download for {}:{}: download took {} ms, cancellation took {} ms", request.getBucket(),
            request.getKey(),
            stopwatch.elapsed(TimeUnit.MILLISECONDS), interruptStopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
    }

    private void handleThrowable(Throwable throwable, Stopwatch stopwatch) {
        final Stopwatch failureStopwatch = Stopwatch.createStarted();
        abortDownload(TransferStatus.FAILED, stopwatch);
        LOG.error("failed download for {}:{}: download took {} ms, handling failure took {} ms", request.getBucket(),
            request.getKey(),
            stopwatch.elapsed(TimeUnit.MILLISECONDS), failureStopwatch.stop().elapsed(TimeUnit.MILLISECONDS),
            throwable);
    }

    private void abortDownload(TransferStatus finalStatus, Stopwatch stopwatch) {
        final Stopwatch abortStopwatch = Stopwatch.createStarted();
        LOG.warn("aborting download for {}:{}", request.getBucket(), request.getKey());
        if (consumerFuture != null) {
            consumerFuture.cancel(true);
        }
        awaitTermination(stop());

        LOG.warn("aborted download for {}:{}; download took {} ms, abort took {} ms", request.getBucket(),
            request.getKey(),
            stopwatch.elapsed(TimeUnit.MILLISECONDS), abortStopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
        notifyListener(finalStatus);
    }

    private class DownloadChunkTask extends ChunkRunnable {

        private final int partNumber;
        private final ByteBuffer byteBuffer;

        private DownloadChunkTask(int partNumber, ByteBuffer byteBuffer) {
            this.partNumber = partNumber;
            this.byteBuffer = byteBuffer;
        }

        @Nonnull
        @Override
        public String getName() {
            return String.format("download part %d of %s:%s", getPartNumber(), request.getBucket(), request.getKey());
        }

        public int getPartNumber() {
            return partNumber;
        }

        @Override
        protected void doRun() {
            LOG.debug("start download part {} for {}:{}", partNumber, request.getBucket(), request.getKey());
            final Stopwatch chunkStopwatch = createStarted();
            final long rangeStart = start + (partNumber - 1) * chunkSize();
            final long rangeEnd = Math.min(start + partNumber * chunkSize(), end);

            checkCanceled();
            consumeContent(request.getBucket(), request.getKey(), rangeStart, rangeEnd, partNumber, chunkStream -> {
                byteBuffer.clear();
                fill(byteBuffer, autoLogStatStream(chunkStream,
                    format("download input stream for part %s for %s:%s", partNumber, request.getBucket(),
                        request.getKey())));
            });

            checkCanceled();
            LOG.debug("complete download part {} size {} bytes for {}:{} in {} ms",
                partNumber, byteBuffer.limit(), request.getBucket(), request.getKey(),
                chunkStopwatch.elapsed(TimeUnit.MILLISECONDS));
            incrementCurrentStatistic();
            notifyListener(TransferStatus.IN_PROGRESS);

            final Stopwatch consumeStopwatch = createStarted();
            try {
                while (true) {
                    checkCanceled();
                    if (isNextChunkNumberToConsume(partNumber)) {
                        LOG.debug("start consume part {} for {}:{}", partNumber, request.getBucket(), request.getKey());
                        producingStream.write(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
                        LOG.debug("complete consume part {} for {}:{} in {} ms", partNumber, request.getBucket(),
                            request.getKey(), consumeStopwatch.elapsed(TimeUnit.MILLISECONDS));
                        return;
                    } else {
                        Uninterruptibles.sleepUninterruptibly(WAIT_QUANTUM, TimeUnit.MILLISECONDS);
                    }
                }
            } catch (IOException e) {
                LOG.debug("fail consume part {} for {}:{} in {} ms cause ex: {}:{}", partNumber, request.getBucket(),
                    request.getKey(),
                    consumeStopwatch.elapsed(TimeUnit.MILLISECONDS), e.getClass().getSimpleName(), e.getMessage());
                Throwables.propagate(e);
            }
        }
    }

}
