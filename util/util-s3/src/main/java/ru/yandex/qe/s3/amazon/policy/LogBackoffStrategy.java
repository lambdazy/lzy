package ru.yandex.qe.s3.amazon.policy;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Established by terry on 15.07.15.
 */
public class LogBackoffStrategy implements RetryPolicy.BackoffStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(LogBackoffStrategy.class);

    private final RetryPolicy.BackoffStrategy delegate;

    public LogBackoffStrategy(RetryPolicy.BackoffStrategy delegate) {
        this.delegate = delegate;
    }

    @Override
    public long delayBeforeNextRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception,
        int retriesAttempted) {
        final long delay = delegate.delayBeforeNextRetry(originalRequest, exception, retriesAttempted);
        if (originalRequest instanceof UploadPartRequest) {
            final UploadPartRequest uploadPartRequest = (UploadPartRequest) originalRequest;
            LOG.warn("delay upload part {} for {}:{} on attempt {} is {} ms", uploadPartRequest.getPartNumber(),
                uploadPartRequest.getBucketName(), uploadPartRequest.getKey(), retriesAttempted, delay);
        } else if (originalRequest instanceof InitiateMultipartUploadRequest) {
            final InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                (InitiateMultipartUploadRequest) originalRequest;
            LOG.warn("delay init upload for {}:{} on attempt {} is {} ms",
                initiateMultipartUploadRequest.getBucketName(), initiateMultipartUploadRequest.getKey(),
                retriesAttempted, delay);
        } else if (originalRequest instanceof CompleteMultipartUploadRequest) {
            final CompleteMultipartUploadRequest completeMultipartUploadRequest =
                (CompleteMultipartUploadRequest) originalRequest;
            LOG.warn("delay complete upload for {}:{} on attempt {} is {} ms",
                completeMultipartUploadRequest.getBucketName(), completeMultipartUploadRequest.getKey(),
                retriesAttempted, delay);
        } else if (originalRequest instanceof AbortMultipartUploadRequest) {
            final AbortMultipartUploadRequest abortMultipartUploadRequest =
                (AbortMultipartUploadRequest) originalRequest;
            LOG.warn("delay abort upload for {}:{} on attempt {} is {} ms", abortMultipartUploadRequest.getBucketName(),
                abortMultipartUploadRequest.getKey(), retriesAttempted, delay);
        } else if (originalRequest instanceof PutObjectRequest) {
            final PutObjectRequest putObjectRequest = (PutObjectRequest) originalRequest;
            LOG.warn("delay put object for {}:{} on attempt {} is {} ms", putObjectRequest.getBucketName(),
                putObjectRequest.getKey(), retriesAttempted, delay);
        } else if (originalRequest instanceof GetObjectMetadataRequest) {
            final GetObjectMetadataRequest getObjectMetadataRequest = (GetObjectMetadataRequest) originalRequest;
            LOG.warn("delay get object metadata for {}:{} on attempt {} is {} ms",
                getObjectMetadataRequest.getBucketName(), getObjectMetadataRequest.getKey(), retriesAttempted, delay);
        } else {
            LOG.warn("delay request {} on attempt {} is {} ms", originalRequest.getClass().getSimpleName(),
                retriesAttempted, delay);
        }
        return delay;
    }
}
