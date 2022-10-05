package ru.yandex.qe.s3.amazon.policy;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy;

import java.util.Collection;

/**
 * Established by terry on 18.12.15.
 */
public class AndRetryCondition implements RetryPolicy.RetryCondition {

    private final Collection<RetryPolicy.RetryCondition> conditions;

    public AndRetryCondition(Collection<RetryPolicy.RetryCondition> conditions) {
        this.conditions = conditions;
    }

    @Override
    public boolean shouldRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception,
        int retriesAttempted) {
        for (RetryPolicy.RetryCondition condition : conditions) {
            if (!condition.shouldRetry(originalRequest, exception, retriesAttempted)) {
                return false;
            }
        }
        return true;
    }
}
