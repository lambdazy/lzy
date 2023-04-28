package ru.yandex.qe.s3.transfer.factories;

import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.annotation.Nonnull;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import ru.yandex.qe.s3.transfer.DownloadTransmitter;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.UploadTransmitter;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferSizeType;
import ru.yandex.qe.s3.transfer.buffers.DynamicByteBufferPool;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

/**
 * Established by terry on 30.01.16.
 */
public abstract class BaseTransmitterFactory {

    protected static final AtomicLong POOL_NUMBER = new AtomicLong();

    protected final ByteBufferSizeType byteBufferSizeType;

    public BaseTransmitterFactory(ByteBufferSizeType byteBufferSizeType) {
        this.byteBufferSizeType = byteBufferSizeType;
    }

    public abstract Transmitter create(@Nonnull ByteBufferPool byteBufferPool,
        @Nonnull ListeningExecutorService transferExecutor,
        @Nonnull ListeningExecutorService chunksExecutor,
        @Nonnull ListeningExecutorService consumeExecutor);

    public UploadTransmitter sameThreadUploadTransmitter() {
        return create(createByteBufferPool("upload", byteBufferSizeType, 1), newDirectExecutorService(),
            newDirectExecutorService(),
            newDirectExecutorService());
    }

    public UploadTransmitter fixedPoolsUploadTransmitter(String transmitterName, int uploadsPoolSize,
        int chunksPoolSize) {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(createByteBufferPool(transmitterName, byteBufferSizeType, uploadsPoolSize + chunksPoolSize),
            fixedThreadPool(transmitterName, uploadsPoolSize, "upload", poolNumber),
            fixedThreadPool(transmitterName, chunksPoolSize, "chunks", poolNumber),
            newDirectExecutorService());
    }

    public DownloadTransmitter sameThreadDownloadTransmitter() {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(createByteBufferPool("download", byteBufferSizeType, 1),
            newDirectExecutorService(),
            //chunk thread should be separated thread, because if consumer exit without reading data,
            // download execution will be blocked on pipe
            listeningDecorator(Executors.newSingleThreadExecutor()),
            createSingleThreadPool("consumer", poolNumber));
    }

    public DownloadTransmitter fixedPoolsDownloadTransmitter(String transmitterName, int downloadsPoolSize,
        int chunksPoolSize) {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(createByteBufferPool(transmitterName, byteBufferSizeType, downloadsPoolSize + chunksPoolSize),
            fixedThreadPool(transmitterName, downloadsPoolSize, "download", poolNumber),
            fixedThreadPool(transmitterName, chunksPoolSize, "chunks", poolNumber),
            fixedThreadPool(transmitterName, downloadsPoolSize, "consumer", poolNumber));
    }

    public Transmitter sameThreadTransmitter() {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(createByteBufferPool("transmitter", byteBufferSizeType, 1),
            newDirectExecutorService(),
            //chunk thread should be separated thread, because if consumer exit without reading data,
            // download execution will be blocked on pipe
            listeningDecorator(Executors.newSingleThreadExecutor()),
            createSingleThreadPool("consumer", poolNumber));
    }

    public Transmitter fixedPoolsTransmitter(String transmitterName, int downloadsPoolSize, int chunksPoolSize) {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(createByteBufferPool(transmitterName, byteBufferSizeType, downloadsPoolSize + chunksPoolSize),
            fixedThreadPool(transmitterName, downloadsPoolSize, "transmitter", poolNumber),
            fixedThreadPool(transmitterName, chunksPoolSize, "chunks", poolNumber),
            fixedThreadPool(transmitterName, downloadsPoolSize, "consumer", poolNumber));
    }

    protected ListeningExecutorService fixedThreadPool(String transmitterName, int size, String poolName,
        long poolNumber) {
        final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
            .namingPattern(transmitterName + "-" + poolNumber + "-" + poolName + "-%d")
            .daemon(true)
            .priority(Thread.NORM_PRIORITY)
            .build();
        return listeningDecorator(createFixedThreadPool(transmitterName, size, poolName, poolNumber, threadFactory));
    }

    protected ListeningExecutorService createSingleThreadPool(String poolName, long poolNumber) {
        final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
            .namingPattern(poolName + "-" + poolNumber + "-%d")
            .daemon(true)
            .priority(Thread.NORM_PRIORITY)
            .build();
        return listeningDecorator(
            new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory));
    }

    protected ThreadPoolExecutor createFixedThreadPool(String transmitterName, int size, String poolName,
        long poolNumber, ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
            threadFactory);
    }

    protected ByteBufferPool createByteBufferPool(String transmitterName, ByteBufferSizeType byteBufferSizeType,
        int size) {
        return new DynamicByteBufferPool(byteBufferSizeType, size);
    }
}
