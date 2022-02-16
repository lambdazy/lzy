package ru.yandex.qe.s3.amazon.transfer.loop;

import static java.lang.String.format;

import com.amazonaws.services.s3.model.AmazonS3Exception;

/**
 * Established by terry on 19.01.16.
 */
public class ErrorLogUtils {

    public static String errorLogDetails(Throwable throwable) {
        if (throwable instanceof AmazonS3Exception) {
            final AmazonS3Exception s3Exception = (AmazonS3Exception) throwable;
            return format("http code %s, error code %s", s3Exception.getStatusCode(), s3Exception.getErrorCode());
        } else {
            return format("%s is not AWS exception", throwable.getClass().getSimpleName());
        }
    }
}
