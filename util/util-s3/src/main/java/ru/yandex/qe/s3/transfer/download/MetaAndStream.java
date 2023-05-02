package ru.yandex.qe.s3.transfer.download;

import jakarta.annotation.Nonnull;
import ru.yandex.qe.s3.transfer.meta.Metadata;

import java.io.InputStream;
import javax.annotation.concurrent.Immutable;

/**
 * Established by terry on 17.07.15.
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
