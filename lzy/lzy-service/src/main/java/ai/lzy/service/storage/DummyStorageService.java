package ai.lzy.service.storage;

import ai.lzy.longrunning.Operation;
import ai.lzy.v1.storage.LSS.GetStorageCredentialsRequest;
import ai.lzy.v1.storage.LSS.GetStorageCredentialsResponse;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Secondary;
import jakarta.inject.Singleton;

@Singleton
@Secondary
public class DummyStorageService implements StorageService {
    @Override
    public void processCreateStorageOperation(String userId, String bucket, Operation operation) {}

    @Override
    public void deleteStorage(String userId, String bucket) {}

    @Override
    public void getStorageCreds(GetStorageCredentialsRequest request,
                                StreamObserver<GetStorageCredentialsResponse> response)
    {}
}
