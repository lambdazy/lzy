package ru.yandex.qe.s3.amazon.policy;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy;

import java.util.List;

/**
 * Established by terry on 01.07.15.
 */
public class StatusCodeRetryCondition implements RetryPolicy.RetryCondition {

    private final List<Integer> badStatusCodes;

    public StatusCodeRetryCondition(List<Integer> badStatusCodes) {
        this.badStatusCodes = badStatusCodes;
    }

    @Override
    public boolean shouldRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception,
        int retriesAttempted) {
        if (exception instanceof AmazonServiceException) {
            AmazonServiceException ase = (AmazonServiceException) exception;
            return badStatusCodes.contains(ase.getStatusCode());
        } else {
            return false;
        }
    }
}
