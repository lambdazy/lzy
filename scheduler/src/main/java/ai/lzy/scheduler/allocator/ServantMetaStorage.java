package ai.lzy.scheduler.allocator;

import javax.annotation.Nullable;

public interface ServantMetaStorage {

    void clear(String workflowId, String servantId);

    void saveMeta(String workflowId, String servantId, String meta);

    @Nullable
    String getMeta(String workflowId, String servantId);
}
