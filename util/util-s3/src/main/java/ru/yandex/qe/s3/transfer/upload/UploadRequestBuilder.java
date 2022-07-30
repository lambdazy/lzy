package ru.yandex.qe.s3.transfer.upload;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;
import ru.yandex.qe.s3.transfer.ThrowingSupplier;
import ru.yandex.qe.s3.transfer.TransferAbortPolicy;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.meta.MetadataBuilder;

/**
 * Established by terry on 01.07.15.
 */
@NotThreadSafe
public class UploadRequestBuilder {

    private String bucket;
    private String key;

    private Metadata objectMetadata = null;
    private ThrowingSupplier<InputStream> streamSupplier;

    private int maxConcurrencyLevel;

    private boolean allowEmptyStream = false;

    private ConcurrencyConflictResolve concurrencyConflictResolve = ConcurrencyConflictResolve.ABORT_THIS_REQUEST;
    private TransferAbortPolicy abortPolicy = TransferAbortPolicy.RETURN_IMMEDIATELY;

    public UploadRequestBuilder() {
    }

    public UploadRequestBuilder(UploadRequest uploadRequest) {
        bucket(uploadRequest.getBucket());
        key(uploadRequest.getKey());
        metadata(uploadRequest.getObjectMetadata());
        stream(uploadRequest.getStreamSupplier());
        maxConcurrency(uploadRequest.getMaxConcurrencyLevel());
        allowEmptyStream(uploadRequest.isAllowEmptyStream());
        concurrencyConflictResolve(uploadRequest.getConcurrencyConflictResolve());
        abortPolicy(uploadRequest.getAbortPolicy());
    }

    public UploadRequestBuilder bucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public UploadRequestBuilder key(String key) {
        this.key = key;
        return this;
    }

    public UploadRequestBuilder metadata(Metadata objectMetadata) {
        this.objectMetadata = objectMetadata;
        return this;
    }

    public UploadRequestBuilder stream(ThrowingSupplier<InputStream> streamSupplier) {
        this.streamSupplier = streamSupplier;
        return this;
    }

    public UploadRequestBuilder maxConcurrency(int level) {
        this.maxConcurrencyLevel = level;
        return this;
    }

    public UploadRequestBuilder allowEmptyStream(boolean allowEmptyStream) {
        this.allowEmptyStream = allowEmptyStream;
        return this;
    }

    public UploadRequestBuilder concurrencyConflictResolve(ConcurrencyConflictResolve concurrencyConflictResolve) {
        this.concurrencyConflictResolve = concurrencyConflictResolve;
        return this;
    }

    public UploadRequestBuilder abortPolicy(TransferAbortPolicy abortPolicy) {
        this.abortPolicy = abortPolicy;
        return this;
    }

    public UploadRequest build() {
        checkNotNull(bucket, "bucket not specified!");
        checkNotNull(key, "key not specified!");
        checkNotNull(streamSupplier, "streamSupplier not specified!");

        return new UploadRequest(bucket, key, objectMetadata != null ? objectMetadata : new MetadataBuilder().build(),
            streamSupplier, maxConcurrencyLevel, allowEmptyStream, concurrencyConflictResolve, abortPolicy);
    }
}
