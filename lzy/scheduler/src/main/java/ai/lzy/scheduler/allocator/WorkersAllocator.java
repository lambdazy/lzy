package ai.lzy.scheduler.allocator;

import ai.lzy.model.operation.Operation;
import ai.lzy.v1.longrunning.LongRunning;

public interface WorkersAllocator {

    AllocateResult allocate(String userId, String workflowName,
                                   String sessionId, Operation.Requirements requirements);

    String createSession(String userId, String workflowName, String idempotencyKey);

    void free(String vmId);

    record AllocateResult(
        LongRunning.Operation allocationOp,
        int workerPort,
        int fsPort
    ) {}
}
