package ai.lzy.servant.portal.s3;

public interface S3Repository<T> {
    void put(String bucket, String key, T value);

    T get(String bucket, String key);

    boolean contains(String bucket, String key);

    void remove(String bucket, String key);
}
