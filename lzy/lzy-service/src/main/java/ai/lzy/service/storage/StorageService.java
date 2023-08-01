package ai.lzy.service.storage;

import ai.lzy.longrunning.Operation;
import ai.lzy.v1.storage.LSS.GetStorageCredentialsRequest;
import ai.lzy.v1.storage.LSS.GetStorageCredentialsResponse;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;

public interface StorageService {
    void processCreateStorageOperation(String userId, String bucket, Operation operation);

    void deleteStorage(@Nullable String userId, String bucket);

    void getStorageCreds(GetStorageCredentialsRequest request, StreamObserver<GetStorageCredentialsResponse> response);
}
