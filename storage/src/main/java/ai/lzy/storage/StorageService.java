package ai.lzy.storage;

import ai.lzy.priv.v2.LzyStorageApi;
import ai.lzy.priv.v2.LzyStorageApi.*;
import ai.lzy.priv.v2.LzyStorageGrpc;
import io.grpc.stub.StreamObserver;

public class StorageService extends LzyStorageGrpc.LzyStorageImplBase {

    @Override
    public void createUserS3Bucket(CreateUserS3BucketRequest request, StreamObserver<CreateUserS3BucketResponse> responseObserver) {
        super.createUserS3Bucket(request, responseObserver);
    }

    @Override
    public void deleteUserS3Bucket(DeleteUserS3BucketRequest request, StreamObserver<DeleteUserS3BucketResponse> responseObserver) {
        super.deleteUserS3Bucket(request, responseObserver);
    }

    @Override
    public void getUserS3BucketCredentials(GetUserS3BucketCredentialsRequest request, StreamObserver<GetUserS3BucketCredentialsResponse> responseObserver) {
        super.getUserS3BucketCredentials(request, responseObserver);
    }
}
