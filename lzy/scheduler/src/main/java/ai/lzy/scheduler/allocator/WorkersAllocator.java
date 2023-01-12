package ai.lzy.scheduler.allocator;

import ai.lzy.model.operation.Operation;
import ai.lzy.v1.longrunning.LongRunning;

public interface WorkersAllocator {

    LongRunning.Operation allocate(String userId, String workflowName,
                                   String sessionId, Operation.Requirements requirements);

    String createSession(String userId, String workflowName);

    void free(String vmId);
}
