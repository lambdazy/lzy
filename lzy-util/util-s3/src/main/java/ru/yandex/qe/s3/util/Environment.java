package ru.yandex.qe.s3.util;

import org.apache.commons.lang3.SystemUtils;

import java.util.Objects;

public class Environment {
    public static String getBucketName() {
        return System.getenv("BUCKET_NAME") != null ? System.getenv("BUCKET_NAME") : "";
    }

    public static String getAccessKey() {
        return System.getenv("ACCESS_KEY") != null ? System.getenv("ACCESS_KEY") : "";
    }

    public static String getSecretKey() {
        return System.getenv("SECRET_KEY") != null ? System.getenv("SECRET_KEY") : "";
    }

    public static String getRegion() {
        return System.getenv("REGION") != null ? System.getenv("REGION") : "";
    }

    public static String getServiceEndpoint() {
        return System.getenv("SERVICE_ENDPOINT") != null ? System.getenv("SERVICE_ENDPOINT") : "";
    }

    public static String getPathStyleAccessEnabled() {
        return System.getenv("PATH_STYLE_ACCESS_ENABLED") != null ?
                System.getenv("PATH_STYLE_ACCESS_ENABLED") : "";
    }

    public static String getLzyWhiteboard() {
        return System.getenv("LZYWHITEBOARD") != null ? System.getenv("LZYWHITEBOARD") : "";
    }

    public static boolean useS3Proxy() {
        return Objects.equals(System.getenv("USE_S3_PROXY"), "true");
    }

    public static String getS3ProxyProvider() {
        return System.getenv("S3_PROXY_PROVIDER") != null ? System.getenv("S3_PROXY_PROVIDER"): "azureblob";
    }

    public static String getS3ProxyIdentity() {
        return System.getenv("S3_PROXY_IDENTITY")  != null ? System.getenv("S3_PROXY_IDENTITY"): "";
    }

    public static String getS3ProxyCredentials() {
        return System.getenv("S3_PROXY_CREDENTIALS")  != null ? System.getenv("S3_PROXY_CREDENTIALS"): "";
    }


}
