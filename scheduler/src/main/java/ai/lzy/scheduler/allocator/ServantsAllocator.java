package ai.lzy.scheduler.allocator;

import ai.lzy.model.Operation;

public interface ServantsAllocator {

    void allocate(String workflowName, String servantId, Operation.Requirements requirements);

    void free(String workflowId, String servantId) throws Exception;
}
