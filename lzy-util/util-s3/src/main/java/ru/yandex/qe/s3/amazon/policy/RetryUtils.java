package ru.yandex.qe.s3.amazon.policy;

import com.amazonaws.AmazonClientException;

/**
 * Established by terry
 * on 18.12.15.
 */
public class RetryUtils {

    public static String extractMsg(AmazonClientException ex) {
        final StringBuilder result = new StringBuilder();
        Throwable cause = ex;
        while (cause != null) {
            result.append("\ncause: ").append(cause.getClass().getSimpleName()).append(":").append(cause.getMessage());
            cause = cause.getCause();
        }
        return result.toString();
    }
}
