package ru.yandex.qe.s3.transfer;

import javax.annotation.concurrent.Immutable;

/**
 * Established by terry
 * on 20.01.16.
 */
@Immutable
public class TransferStatistic {
    public static final long UNDEFINED_LENGTH = -1;

    private final long objectContentLength;

    private final long chunkSize;
    private final long chunksCount;
    private final long chunksTransferred;

    public TransferStatistic(long objectContentLength, long chunkSize, long chunksCount, long chunksTransferred) {
        this.objectContentLength = objectContentLength;
        this.chunkSize = chunkSize;
        this.chunksCount = chunksCount;
        this.chunksTransferred = chunksTransferred;
    }

    public long getObjectContentLength() {
        return objectContentLength;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public long getExpectedChunksCount() {
        return chunksCount;
    }

    public long getChunksTransferred() {
        return chunksTransferred;
    }

    @Override
    public String toString() {
        return "TransferStatistic{" +
                "objectContentLength=" + objectContentLength +
                ", chunkSize=" + chunkSize +
                ", chunksCount=" + chunksCount +
                ", chunksTransferred=" + chunksTransferred +
                '}';
    }
}
