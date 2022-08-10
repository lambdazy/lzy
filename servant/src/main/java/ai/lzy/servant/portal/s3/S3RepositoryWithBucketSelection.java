package ai.lzy.servant.portal.s3;

import javax.annotation.Nonnull;

public interface S3RepositoryWithBucketSelection<T> {
    void put(@Nonnull String bucket, @Nonnull String key, @Nonnull T value);

    T get(@Nonnull String bucket, @Nonnull String key);

    void remove(@Nonnull String bucket, @Nonnull String key);
}
