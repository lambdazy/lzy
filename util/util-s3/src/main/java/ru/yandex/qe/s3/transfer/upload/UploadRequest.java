package ru.yandex.qe.s3.transfer.upload;

import jakarta.annotation.Nonnull;
import ru.yandex.qe.s3.transfer.ThrowingSupplier;
import ru.yandex.qe.s3.transfer.TransferAbortPolicy;
import ru.yandex.qe.s3.transfer.meta.Metadata;

import java.io.InputStream;
import javax.annotation.concurrent.Immutable;

/**
 * Established by terry on 01.07.15.
 */
@Immutable
public class UploadRequest {

    private final String bucket;
    private final String key;

    private final Metadata objectMetadata;
    private final ThrowingSupplier<InputStream> streamSupplier;

    private final int maxConcurrencyLevel;

    private final boolean allowEmptyStream;

    private final ConcurrencyConflictResolve concurrencyConflictResolve;
    private final TransferAbortPolicy abortPolicy;

    public UploadRequest(@Nonnull String bucket, @Nonnull String key, @Nonnull Metadata objectMetadata,
        @Nonnull ThrowingSupplier<InputStream> streamSupplier, int maxConcurrencyLevel, boolean allowEmptyStream,
        @Nonnull ConcurrencyConflictResolve concurrencyConflictResolve) {
        this(bucket, key, objectMetadata, streamSupplier, maxConcurrencyLevel, allowEmptyStream,
            concurrencyConflictResolve, TransferAbortPolicy.RETURN_IMMEDIATELY);
    }

    public UploadRequest(@Nonnull String bucket, @Nonnull String key, @Nonnull Metadata objectMetadata,
        @Nonnull ThrowingSupplier<InputStream> streamSupplier, int maxConcurrencyLevel, boolean allowEmptyStream,
        @Nonnull ConcurrencyConflictResolve concurrencyConflictResolve, @Nonnull TransferAbortPolicy abortPolicy) {
        this.bucket = bucket;
        this.key = key;
        this.objectMetadata = objectMetadata;
        this.streamSupplier = streamSupplier;
        this.maxConcurrencyLevel = maxConcurrencyLevel;
        this.allowEmptyStream = allowEmptyStream;
        this.concurrencyConflictResolve = concurrencyConflictResolve;
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

    @Nonnull
    public Metadata getObjectMetadata() {
        return objectMetadata;
    }

    @Nonnull
    public ThrowingSupplier<InputStream> getStreamSupplier() {
        return streamSupplier;
    }

    public int getMaxConcurrencyLevel() {
        return maxConcurrencyLevel;
    }

    public boolean isAllowEmptyStream() {
        return allowEmptyStream;
    }

    @Nonnull
    public ConcurrencyConflictResolve getConcurrencyConflictResolve() {
        return concurrencyConflictResolve;
    }

    @Nonnull
    public TransferAbortPolicy getAbortPolicy() {
        return abortPolicy;
    }

    @Override
    public String toString() {
        return "UploadRequest{"
            + "bucket='" + bucket + '\''
            + ", key='" + key + '\''
            + ", objectMetadata=" + objectMetadata
            + ", maxConcurrencyLevel=" + maxConcurrencyLevel
            + ", allowEmptyStream=" + allowEmptyStream
            + ", concurrencyConflictResolve=" + concurrencyConflictResolve
            + ", abortPolicy=" + abortPolicy
            + '}';
    }
}
