package ai.lzy.storage;

import ai.lzy.longrunning.Operation;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LSS.CreateStorageRequest;
import ai.lzy.v1.storage.LSS.DeleteStorageRequest;
import ai.lzy.v1.storage.LSS.GetStorageCredentialsRequest;
import ai.lzy.v1.storage.LSS.GetStorageCredentialsResponse;
import io.grpc.stub.StreamObserver;

public interface StorageService {
    void processCreateStorageOperation(CreateStorageRequest request, Operation operation,
                                       StreamObserver<LongRunning.Operation> response);

    void deleteStorage(DeleteStorageRequest request, StreamObserver<LSS.DeleteStorageResponse> response);

    void getStorageCreds(GetStorageCredentialsRequest request, StreamObserver<GetStorageCredentialsResponse> response);
}
