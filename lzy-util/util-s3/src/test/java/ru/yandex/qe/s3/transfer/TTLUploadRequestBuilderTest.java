package ru.yandex.qe.s3.transfer;

import java.io.ByteArrayInputStream;
import org.hamcrest.core.Is;
import org.joda.time.Duration;
import org.junit.Assert;
import org.testng.annotations.Test;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.meta.MetadataBuilder;
import ru.yandex.qe.s3.transfer.ttl.TTLUploadRequest;
import ru.yandex.qe.s3.transfer.ttl.TTLUploadRequestBuilder;

/**
 * Established by terry on 30.07.15.
 */
public class TTLUploadRequestBuilderTest {

    @Test(expectedExceptions = NullPointerException.class)
    public void fail_if_ttl_not_set() {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        new TTLUploadRequestBuilder().key("key").bucket("bucket")
            .stream(StreamSuppliers.of(inputStream))
            .build();
    }

    @Test
    public void ok_creating_with_all_params() {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        final Metadata objectMetadata = new MetadataBuilder().setObjectContentLength(10).build();

        final Duration ttl = new Duration(1000);
        final TTLUploadRequest request = new TTLUploadRequestBuilder().ttl(ttl).key("key").bucket("bucket")
            .maxConcurrency(4)
            .metadata(objectMetadata)
            .stream(StreamSuppliers.of(inputStream))
            .build();

        Assert.assertThat(request.getTTL(), Is.is(ttl));
        Assert.assertThat(request.getKey(), Is.is("key"));
        Assert.assertThat(request.getBucket(), Is.is("bucket"));
        Assert.assertThat(request.getStreamSupplier().get(), Is.is(inputStream));
        Assert.assertThat(request.getMaxConcurrencyLevel(), Is.is(4));
        Assert.assertEquals(request.getObjectMetadata().getMetadata(), objectMetadata.getMetadata());
    }
}
