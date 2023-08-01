package ai.lzy.service.storage;

import ai.lzy.longrunning.Operation;
import jakarta.annotation.Nullable;

public interface StorageService {
    void processCreateStorageOperation(String userId, String bucket, Operation operation);

    void deleteStorage(@Nullable String userId, String bucket);
}
