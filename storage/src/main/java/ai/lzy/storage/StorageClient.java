package ai.lzy.storage;

import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc.LzyStorageServiceBlockingStub;
import io.grpc.StatusRuntimeException;
import io.micronaut.core.util.StringUtils;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StorageClient {
    private static final Logger LOG = LogManager.getLogger(StorageClient.class);

    @Nullable
    public static LMS3.S3Locator getOrCreateTempUserBucket(String userId, LzyStorageServiceBlockingStub grpcClient) {
        var bucketName = "tmp-bucket-" + userId;

        LOG.info("Creating new temp storage bucket '{}' for user '{}'", bucketName, userId);

        LSS.CreateS3BucketResponse response = grpcClient.createS3Bucket(
            LSS.CreateS3BucketRequest.newBuilder()
                .setUserId(userId)
                .setBucket(bucketName)
                .build());

        return switch (response.getCredentialsCase()) {
            case AMAZON -> LMS3.S3Locator.newBuilder().setAmazon(response.getAmazon()).setBucket(bucketName).build();
            case AZURE -> LMS3.S3Locator.newBuilder().setAzure(response.getAzure()).setBucket(bucketName).build();
            default -> {
                LOG.error("Unsupported bucket storage type {}", response.getCredentialsCase());
                deleteTempUserBucket(bucketName, grpcClient);
                yield null;
            }
        };
    }

    public static void deleteTempUserBucket(String bucket, LzyStorageServiceBlockingStub grpcClient) {
        if (StringUtils.isEmpty(bucket)) {
            return;
        }

        LOG.info("Deleting temp storage bucket '{}'", bucket);

        try {
            @SuppressWarnings("unused")
            var resp = grpcClient.deleteS3Bucket(
                LSS.DeleteS3BucketRequest.newBuilder()
                    .setBucket(bucket)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Can't delete temp bucket '{}': ({}) {}", bucket, e.getStatus(), e.getMessage(), e);
        }
    }
}
