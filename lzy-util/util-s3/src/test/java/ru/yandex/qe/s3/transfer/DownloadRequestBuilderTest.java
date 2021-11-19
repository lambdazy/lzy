package ru.yandex.qe.s3.transfer;

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.springframework.test.context.ActiveProfiles;
import org.testng.annotations.Test;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;

import static org.junit.Assert.assertEquals;
import static ru.yandex.qe.s3.transfer.download.DownloadRequest.UNDEFF_BOUND_VALUE;

/**
 * Established by terry
 * on 30.07.15.
 */
@ActiveProfiles("testing")
public class DownloadRequestBuilderTest {

    @Test (expectedExceptions = NullPointerException.class)
    public void fail_to_create_if_key_not_set() {
        new DownloadRequestBuilder().bucket("bucket").build();
    }

    @Test (expectedExceptions = NullPointerException.class)
    public void fail_to_create_if_bucket_not_set() {
        new DownloadRequestBuilder().key("key").build();
    }

    @Test
    public void ok_creating_with_key_and_bucket() {
        final DownloadRequest request = new DownloadRequestBuilder().key("key").bucket("bucket").build();

        Assert.assertThat(request.getKey(), Is.is("key"));
        Assert.assertThat(request.getBucket(), Is.is("bucket"));
        Assert.assertThat(request.getMaxConcurrencyLevel(), Is.is(0));
        Assert.assertEquals(request.getStart(), UNDEFF_BOUND_VALUE);
        Assert.assertEquals(request.getEnd(), UNDEFF_BOUND_VALUE);
    }

    @Test
    public void ok_creating_with_all_params() {
        final DownloadRequest request = new DownloadRequestBuilder().key("key").bucket("bucket")
                .maxConcurrency(3).range(10, 20).build();

        Assert.assertThat(request.getKey(), Is.is("key"));
        Assert.assertThat(request.getBucket(), Is.is("bucket"));
        Assert.assertThat(request.getMaxConcurrencyLevel(), Is.is(3));
        assertEquals(request.getStart(), 10l);
        assertEquals(request.getEnd(), 20l);
    }
}
