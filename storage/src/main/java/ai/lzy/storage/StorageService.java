package ai.lzy.storage;

import ai.lzy.longrunning.Operation;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LSS.CreateS3BucketRequest;
import ai.lzy.v1.storage.LSS.DeleteS3BucketRequest;
import ai.lzy.v1.storage.LSS.GetS3BucketCredentialsRequest;
import ai.lzy.v1.storage.LSS.GetS3BucketCredentialsResponse;
import io.grpc.stub.StreamObserver;

public interface StorageService {
    void processCreateBucketOperation(CreateS3BucketRequest request, Operation operation,
                                      StreamObserver<LongRunning.Operation> response);

    void deleteBucket(DeleteS3BucketRequest request, StreamObserver<LSS.DeleteS3BucketResponse> response);

    void getBucketCreds(GetS3BucketCredentialsRequest request, StreamObserver<GetS3BucketCredentialsResponse> response);
}
