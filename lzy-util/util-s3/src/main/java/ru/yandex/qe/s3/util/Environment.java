package ru.yandex.qe.s3.util;

import org.apache.commons.lang3.SystemUtils;

import java.util.Objects;

public class Environment {
    private final static String internalHost = SystemUtils.IS_OS_LINUX ? "localhost" : "host.docker.internal";

    public static String getBucketName() {
        return System.getenv("BUCKET_NAME") != null ? System.getenv("BUCKET_NAME") : "lzy-bucket";
    }

    public static String getAccessKey() {
        return System.getenv("ACCESS_KEY") != null ? System.getenv("ACCESS_KEY") : "access-key";
    }

    public static String getSecretKey() {
        return System.getenv("SECRET_KEY") != null ? System.getenv("SECRET_KEY") : "secret-key";
    }

    public static String getRegion() {
        return System.getenv("REGION") != null ? System.getenv("REGION") : "us-west-2";
    }

    public static String getServiceEndpoint() {
        return System.getenv("SERVICE_ENDPOINT") != null ? System.getenv("SERVICE_ENDPOINT") : "http://" + internalHost +":8001";
    }

    public static String getPathStyleAccessEnabled() {
        return System.getenv("PATH_STYLE_ACCESS_ENABLED") != null ? System.getenv("PATH_STYLE_ACCESS_ENABLED") : "true";
    }

    public static String getLzyWhiteboard() {
        return System.getenv("LZYWHITEBOARD") != null ? System.getenv("LZYWHITEBOARD") : "http://" + internalHost +":8999";
    }

    public static boolean useS3Proxy() {
        return Objects.equals(System.getenv("USE_S3_PROXY"), "true");
    }

    public static String getS3ProxyProvider() {
        return System.getenv("S3_PROXY_PROVIDER");
    }

    public static String getS3ProxyIdentity() {
        return System.getenv("S3_PROXY_IDENTITY");
    }

    public static String getS3ProxyCredentials() {
        return System.getenv("S3_PROXY_CREDENTIALS");
    }


}
