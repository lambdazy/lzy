package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

public class Environment {
    public static String getBucketName() {
        return System.getenv("BUCKET_NAME");
    }

    public static String getAccessKey() {
        return System.getenv("ACCESS_KEY");
    }

    public static String getSecretKey() {
        return System.getenv("SECRET_KEY");
    }

    public static String getRegion() {
        return System.getenv("REGION");
    }

    public static String getServiceEndpoint() {
        return System.getenv("SERVICE_ENDPOINT");
    }

    public static String getPathStyleAccessEnabled() {
        return System.getenv("PATH_STYLE_ACCESS_ENABLED");
    }
}
