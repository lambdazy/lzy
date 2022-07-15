package ai.lzy.storage;

import ai.lzy.priv.v2.LzyStorageApi.*;
import ai.lzy.priv.v2.LzyStorageGrpc;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class StorageService extends LzyStorageGrpc.LzyStorageImplBase {

    @Inject
    public StorageService(StorageConfig config) {
        config.validate();


    }

    @Override
    public void createS3Bucket(CreateS3BucketRequest request, StreamObserver<CreateS3BucketResponse> response) {
        super.createS3Bucket(request, response);
    }

    @Override
    public void deleteS3Bucket(DeleteS3BucketRequest request, StreamObserver<DeleteS3BucketResponse> response) {
        super.deleteS3Bucket(request, response);
    }

    @Override
    public void getS3BucketCredentials(GetS3BucketCredentialsRequest request,
                                       StreamObserver<GetS3BucketCredentialsResponse> response) {
        super.getS3BucketCredentials(request, response);
    }
}
