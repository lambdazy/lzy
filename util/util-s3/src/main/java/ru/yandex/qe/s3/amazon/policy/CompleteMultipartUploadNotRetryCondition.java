package ru.yandex.qe.s3.amazon.policy;

import static ru.yandex.qe.s3.amazon.policy.RetryUtils.extractMsg;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Established by terry on 18.12.15.
 */
public class CompleteMultipartUploadNotRetryCondition implements RetryPolicy.RetryCondition {

    private static final Logger LOG = LoggerFactory.getLogger(CompleteMultipartUploadNotRetryCondition.class);

    @Override
    public boolean shouldRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception,
        int retriesAttempted) {
        if (originalRequest instanceof CompleteMultipartUploadRequest) {
            final CompleteMultipartUploadRequest completeMultipartUploadRequest =
                (CompleteMultipartUploadRequest) originalRequest;
            LOG.warn("fail complete upload for {}:{} on attempt {} causes: {}",
                completeMultipartUploadRequest.getBucketName(),
                completeMultipartUploadRequest.getKey(), retriesAttempted, extractMsg(exception));
            return false;
        }
        return true;
    }
}
