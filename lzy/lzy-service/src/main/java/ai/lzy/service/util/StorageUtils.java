package ai.lzy.service.util;

public enum StorageUtils {
    ;

    public static String createInternalBucketName(String userId) {
        return "tmp-bucket-" + userId;
    }
}
