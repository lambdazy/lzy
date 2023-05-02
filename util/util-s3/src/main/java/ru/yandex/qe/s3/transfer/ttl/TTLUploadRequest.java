package ru.yandex.qe.s3.transfer.ttl;

import jakarta.annotation.Nonnull;
import org.joda.time.Duration;
import ru.yandex.qe.s3.transfer.ThrowingSupplier;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.upload.ConcurrencyConflictResolve;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;

import java.io.InputStream;

/**
 * Established by terry on 14.07.15.
 */
public class TTLUploadRequest extends UploadRequest {

    private final Duration ttl;

    public TTLUploadRequest(@Nonnull String bucket, @Nonnull String key,
        @Nonnull Metadata objectMetadata, @Nonnull ThrowingSupplier<InputStream> streamSupplier,
        int maxConcurrencyLevel,
        boolean allowEmptyStream, @Nonnull ConcurrencyConflictResolve concurrencyConflictResolve,
        @Nonnull Duration ttl) {
        super(bucket, key, objectMetadata, streamSupplier, maxConcurrencyLevel, allowEmptyStream,
            concurrencyConflictResolve);
        this.ttl = ttl;
    }

    @Nonnull
    public Duration getTTL() {
        return ttl;
    }
}
