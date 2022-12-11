package ai.lzy.scheduler.allocator;

import ai.lzy.model.operation.Operation;

public interface WorkersAllocator {

    void allocate(String userId, String workflowName, String workerId, Operation.Requirements requirements);

    void free(String workflowId, String workerId) throws Exception;
}
