package ru.yandex.qe.s3.transfer;

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.meta.MetadataBuilder;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadRequestBuilder;

import java.io.ByteArrayInputStream;

/**
 * Established by terry on 30.07.15.
 */
public class UploadRequestBuilderTest {

    @Test(expected = NullPointerException.class)
    public void fail_to_create_if_key_not_set() {
        new UploadRequestBuilder().bucket("bucket").build();
    }

    @Test(expected = NullPointerException.class)
    public void fail_to_create_if_bucket_not_set() {
        new UploadRequestBuilder().key("key").build();
    }

    @Test(expected = NullPointerException.class)
    public void fail_to_create_if_stream_supplier_not_set() {
        new UploadRequestBuilder().key("key").bucket("bucket").build();
    }

    @Test
    public void ok_creating_with_key_and_bucket_and_stream() {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        final UploadRequest request = new UploadRequestBuilder().key("key").bucket("bucket")
            .stream(StreamSuppliers.of(inputStream)).build();

        Assert.assertThat(request.getKey(), Is.is("key"));
        Assert.assertThat(request.getBucket(), Is.is("bucket"));
        Assert.assertThat(request.getStreamSupplier().get(), Is.is(inputStream));
        Assert.assertThat(request.getMaxConcurrencyLevel(), Is.is(0));
        Assert.assertTrue(request.getObjectMetadata().getMetadata().isEmpty());
    }

    @Test
    public void ok_creating_with_all_params() {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        final Metadata objectMetadata = new MetadataBuilder().setObjectContentLength(10).build();

        final UploadRequest request = new UploadRequestBuilder().key("key").bucket("bucket")
            .maxConcurrency(4)
            .metadata(objectMetadata)
            .stream(StreamSuppliers.of(inputStream)).build();

        Assert.assertThat(request.getKey(), Is.is("key"));
        Assert.assertThat(request.getBucket(), Is.is("bucket"));
        Assert.assertThat(request.getStreamSupplier().get(), Is.is(inputStream));
        Assert.assertThat(request.getMaxConcurrencyLevel(), Is.is(4));
        Assert.assertEquals(request.getObjectMetadata().getMetadata(), objectMetadata.getMetadata());
    }
}
