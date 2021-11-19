package ru.yandex.qe.s3.transfer.download;

import ru.yandex.qe.s3.transfer.meta.Metadata;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.io.InputStream;

/**
 * Established by terry
 * on 17.07.15.
 */
@Immutable
public class MetaAndStream {

    private final Metadata objectMetadata;
    private final InputStream inputStream;

    public MetaAndStream(Metadata objectMetadata, InputStream inputStream) {
        this.objectMetadata = objectMetadata;
        this.inputStream = inputStream;
    }

    @Nonnull
    public Metadata getObjectMetadata() {
        return objectMetadata;
    }

    @Nonnull
    public InputStream getInputStream() {
        return inputStream;
    }
}
