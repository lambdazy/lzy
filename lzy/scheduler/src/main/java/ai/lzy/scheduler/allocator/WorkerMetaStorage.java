package ai.lzy.scheduler.allocator;

import javax.annotation.Nullable;

public interface WorkerMetaStorage {

    void clear(String workflowName, String workerId);

    void saveMeta(String workflowName, String workerId, String meta);

    @Nullable
    String getMeta(String workflowName, String workerId);
}
