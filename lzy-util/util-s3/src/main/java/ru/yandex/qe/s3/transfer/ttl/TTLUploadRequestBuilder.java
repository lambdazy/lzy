package ru.yandex.qe.s3.transfer.ttl;

import org.joda.time.Duration;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.ThrowingSupplier;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadRequestBuilder;

import java.io.InputStream;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * Established by terry
 * on 14.07.15.
 */
public class TTLUploadRequestBuilder extends UploadRequestBuilder {
    private Duration ttl;

    public TTLUploadRequestBuilder() {
        super();
    }

    public TTLUploadRequestBuilder(UploadRequest uploadRequest) {
        super(uploadRequest);
    }

    public TTLUploadRequestBuilder ttl(Duration ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public TTLUploadRequestBuilder bucket(String bucket) {
        return (TTLUploadRequestBuilder) super.bucket(bucket);
    }

    @Override
    public TTLUploadRequestBuilder key(String key) {
        return (TTLUploadRequestBuilder) super.key(key);
    }

    @Override
    public TTLUploadRequestBuilder metadata(Metadata objectMetadata) {
        return (TTLUploadRequestBuilder) super.metadata(objectMetadata);
    }

    @Override
    public TTLUploadRequestBuilder stream(ThrowingSupplier<InputStream> streamSupplier) {
        return (TTLUploadRequestBuilder) super.stream(streamSupplier);
    }

    @Override
    public TTLUploadRequestBuilder maxConcurrency(int level) {
        return (TTLUploadRequestBuilder) super.maxConcurrency(level);
    }

    @Override
    public TTLUploadRequest build() {
        checkNotNull(ttl, "ttl not specified!");
        final UploadRequest baseRequest = super.build();
        return new TTLUploadRequest(baseRequest.getBucket(), baseRequest.getKey(),
                baseRequest.getObjectMetadata(), baseRequest.getStreamSupplier(),
                baseRequest.getMaxConcurrencyLevel(), baseRequest.isAllowEmptyStream(), baseRequest.getConcurrencyConflictResolve(), ttl);
    }
}
