package ru.yandex.qe.s3.transfer.download;

import jakarta.annotation.Nonnull;
import ru.yandex.qe.s3.transfer.TransferAbortPolicy;

import javax.annotation.concurrent.Immutable;

/**
 * Established by terry on 16.07.15.
 */
@Immutable
public class DownloadRequest {

    public static final int UNDEFF_BOUND_VALUE = -1;

    private final String bucket;
    private final String key;

    private final long start;
    private final long end;

    private final int maxConcurrencyLevel;

    private final TransferAbortPolicy abortPolicy;

    public DownloadRequest(@Nonnull String bucket, @Nonnull String key, long start, long end, int maxConcurrencyLevel) {
        this(bucket, key, start, end, maxConcurrencyLevel, TransferAbortPolicy.RETURN_IMMEDIATELY);
    }

    public DownloadRequest(@Nonnull String bucket, @Nonnull String key, long start, long end, int maxConcurrencyLevel,
        @Nonnull TransferAbortPolicy abortPolicy) {
        this.bucket = bucket;
        this.key = key;
        this.start = start;
        this.end = end;
        this.maxConcurrencyLevel = maxConcurrencyLevel;
        this.abortPolicy = abortPolicy;
    }

    @Nonnull
    public String getBucket() {
        return bucket;
    }

    @Nonnull
    public String getKey() {
        return key;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public int getMaxConcurrencyLevel() {
        return maxConcurrencyLevel;
    }

    @Nonnull
    public TransferAbortPolicy getAbortPolicy() {
        return abortPolicy;
    }

    @Override
    public String toString() {
        return "DownloadRequest{"
            + "bucket='" + bucket + '\''
            + ", key='" + key + '\''
            + ", start=" + start
            + ", end=" + end
            + ", maxConcurrencyLevel=" + maxConcurrencyLevel
            + ", abortPolicy=" + abortPolicy
            + '}';
    }
}
