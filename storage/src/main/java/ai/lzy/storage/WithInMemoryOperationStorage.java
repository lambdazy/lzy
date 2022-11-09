package ai.lzy.storage;

import ai.lzy.longrunning.Operation;

import java.util.Map;

public interface WithInMemoryOperationStorage {
    /**
     * Returns map of Idempotency Key --> Operation
     */
    Map<String, Operation> getOperations();
}
