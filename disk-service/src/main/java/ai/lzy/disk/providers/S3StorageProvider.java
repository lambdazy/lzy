package ai.lzy.disk.providers;

import ai.lzy.disk.model.DiskType;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.disk.model.DiskSpec;
import ai.lzy.disk.model.S3StorageSpec;

@Singleton
@Requires(beans = AmazonS3.class)
public class S3StorageProvider implements DiskStorageProvider {

    private static final Logger LOG = LogManager.getLogger(S3StorageProvider.class);
    private static final DiskType PROVIDER_TYPE = DiskType.S3_STORAGE;

    private final AmazonS3 s3Client;

    @Inject
    public S3StorageProvider(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public DiskType getType() {
        return PROVIDER_TYPE;
    }

    @Override
    public S3StorageSpec createDisk(String label, String diskId, int diskSizeGb) {
        String bucket = genBucketName(label, diskId);
        doS3Request(
            () -> s3Client.createBucket(bucket),
            "Failed to create bucket " + bucket
        );
        return new S3StorageSpec(diskSizeGb, bucket);
    }

    @Override
    public boolean isExistDisk(DiskSpec diskSpec) {
        S3StorageSpec s3Spec = assertDiskSpec(diskSpec);
        return doS3Request(
            () -> s3Client.doesBucketExistV2(s3Spec.bucket()),
            "Failed to check if bucket " + s3Spec.bucket() + " exists"
        );
    }

    @Override
    public void deleteDisk(DiskSpec diskSpec) {
        S3StorageSpec s3Spec = assertDiskSpec(diskSpec);
        doS3Request(
            () -> s3Client.deleteBucket(s3Spec.bucket()),
            "Failed to delete bucket " + s3Spec.bucket()
        );
    }

    private String genBucketName(String label, String diskId) {
        return label.replaceAll("[^-a-z0-9]", "-") + "-" + diskId;
    }

    private S3StorageSpec assertDiskSpec(DiskSpec spec) {
        if (!PROVIDER_TYPE.equals(spec.type())) {
            String errorMessage = "Unexpected disk spec of type " + spec.type();
            LOG.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
        return (S3StorageSpec) spec;
    }

    private void doS3Request(Runnable s3Request, String messageOnError) {
        doS3Request(() -> {
            s3Request.run();
            return true;
        }, messageOnError);
    }

    private <T> T doS3Request(Supplier<T> s3Request, String messageOnError) {
        try {
            return s3Request.get();
        } catch (AmazonClientException e) {
            LOG.error(messageOnError);
            throw new RuntimeException(messageOnError, e);
        }
    }

}
