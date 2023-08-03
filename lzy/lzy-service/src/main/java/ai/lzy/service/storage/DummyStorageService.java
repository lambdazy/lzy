package ai.lzy.service.storage;

import ai.lzy.longrunning.Operation;
import io.micronaut.context.annotation.Secondary;
import jakarta.inject.Singleton;

@Singleton
@Secondary
public class DummyStorageService implements StorageService {
    @Override
    public void processCreateStorageOperation(String userId, String bucket, Operation operation) {}

    @Override
    public void deleteStorage(String userId, String bucket) {}
}
