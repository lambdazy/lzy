package ai.lzy.scheduler.allocator;

import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;

public interface WorkersAllocator {

    LongRunning.Operation allocate(String userId, String workflowName,
                                   String sessionId, LMO.Requirements requirements);

    String createSession(String userId, String workflowName, String idempotencyKey);

    void free(String vmId);
}
