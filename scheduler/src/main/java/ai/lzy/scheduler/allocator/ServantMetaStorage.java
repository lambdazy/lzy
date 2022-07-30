package ai.lzy.scheduler.allocator;

import javax.annotation.Nullable;

public interface ServantMetaStorage {

    void clear(String workflowName, String servantId);

    void saveMeta(String workflowName, String servantId, String meta);

    @Nullable
    String getMeta(String workflowName, String servantId);
}
