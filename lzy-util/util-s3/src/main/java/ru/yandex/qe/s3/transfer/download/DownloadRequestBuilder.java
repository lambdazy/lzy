package ru.yandex.qe.s3.transfer.download;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static ru.yandex.qe.s3.transfer.download.DownloadRequest.UNDEFF_BOUND_VALUE;

import ru.yandex.qe.s3.transfer.TransferAbortPolicy;

/**
 * Established by terry
 * on 16.07.15.
 */
@NotThreadSafe
public class DownloadRequestBuilder {
    private String bucket;
    private String key;

    private long start = UNDEFF_BOUND_VALUE;
    private long end = UNDEFF_BOUND_VALUE;

    private int maxConcurrencyLevel;

    private TransferAbortPolicy abortPolicy = TransferAbortPolicy.RETURN_IMMEDIATELY;

    public DownloadRequestBuilder bucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public DownloadRequestBuilder key(String key) {
        this.key = key;
        return this;
    }

    public DownloadRequestBuilder range(long start, long endInclusive) {
        this.start = start;
        this.end = endInclusive;
        return this;
    }

    public DownloadRequestBuilder maxConcurrency(int level) {
        this.maxConcurrencyLevel = level;
        return this;
    }

    public DownloadRequestBuilder abortPolicy(TransferAbortPolicy abortPolicy) {
        this.abortPolicy = abortPolicy;
        return this;
    }

    public DownloadRequest build() {
        checkNotNull(bucket, "bucket not specified!");
        checkNotNull(key, "key not specified!");
        if (start != UNDEFF_BOUND_VALUE) {
            checkArgument(start>= 0, format("start must no negative, but is %s", start));
        }
        if (end != UNDEFF_BOUND_VALUE) {
            checkArgument(start <= end, format("end must be <= then start but start is %s, and end is %s", start, end));
        }
        return new DownloadRequest(bucket, key, start, end, maxConcurrencyLevel, abortPolicy);
    }
}
