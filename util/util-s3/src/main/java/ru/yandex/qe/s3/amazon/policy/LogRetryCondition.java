package ru.yandex.qe.s3.amazon.policy;

import static ru.yandex.qe.s3.amazon.policy.RetryUtils.extractMsg;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Established by terry on 14.07.15.
 */
public class LogRetryCondition implements RetryPolicy.RetryCondition {

    private static final Logger LOG = LoggerFactory.getLogger(LogRetryCondition.class);

    private final RetryPolicy.RetryCondition delegate;

    public LogRetryCondition(RetryPolicy.RetryCondition delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean shouldRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception,
        int retriesAttempted) {
        final boolean shouldRetry = delegate.shouldRetry(originalRequest, exception, retriesAttempted);
        if (shouldRetry) {
            if (originalRequest instanceof UploadPartRequest) {
                final UploadPartRequest uploadPartRequest = (UploadPartRequest) originalRequest;
                LOG.warn("fail upload part {} for {}:{} on attempt {} causes: {}", uploadPartRequest.getPartNumber(),
                    uploadPartRequest.getBucketName(), uploadPartRequest.getKey(), retriesAttempted,
                    extractMsg(exception));
            } else if (originalRequest instanceof InitiateMultipartUploadRequest) {
                final InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                    (InitiateMultipartUploadRequest) originalRequest;
                LOG.warn("fail init upload for {}:{} on attempt {} causes: {}",
                    initiateMultipartUploadRequest.getBucketName(), initiateMultipartUploadRequest.getKey(),
                    retriesAttempted, extractMsg(exception));
            } else if (originalRequest instanceof CompleteMultipartUploadRequest) {
                final CompleteMultipartUploadRequest completeMultipartUploadRequest =
                    (CompleteMultipartUploadRequest) originalRequest;
                LOG.warn("fail complete upload for {}:{} on attempt {} causes: {}",
                    completeMultipartUploadRequest.getBucketName(), completeMultipartUploadRequest.getKey(),
                    retriesAttempted, extractMsg(exception));
            } else if (originalRequest instanceof AbortMultipartUploadRequest) {
                final AbortMultipartUploadRequest abortMultipartUploadRequest =
                    (AbortMultipartUploadRequest) originalRequest;
                LOG.warn("fail abort upload for {}:{} on attempt {} causes: {}",
                    abortMultipartUploadRequest.getBucketName(), abortMultipartUploadRequest.getKey(), retriesAttempted,
                    extractMsg(exception));
            } else if (originalRequest instanceof PutObjectRequest) {
                final PutObjectRequest putObjectRequest = (PutObjectRequest) originalRequest;
                LOG.warn("fail put object for {}:{} on attempt {} causes: {}", putObjectRequest.getBucketName(),
                    putObjectRequest.getKey(), retriesAttempted, extractMsg(exception));
            } else if (originalRequest instanceof GetObjectMetadataRequest) {
                final GetObjectMetadataRequest getObjectMetadataRequest = (GetObjectMetadataRequest) originalRequest;
                LOG.warn("fail get object metadata for {}:{} on attempt {} causes: {}",
                    getObjectMetadataRequest.getBucketName(), getObjectMetadataRequest.getKey(), retriesAttempted,
                    extractMsg(exception));
            } else {
                LOG.warn("fail request {} on attempt {} causes: {}", originalRequest.getClass().getSimpleName(),
                    retriesAttempted, extractMsg(exception));
            }
        }
        return shouldRetry;
    }
}
