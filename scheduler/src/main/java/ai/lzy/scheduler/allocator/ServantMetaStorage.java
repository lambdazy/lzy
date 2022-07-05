package ai.lzy.scheduler.allocator;

import javax.annotation.Nullable;

public interface ServantMetaStorage {

    String generateToken(String workflowId, String servantId);
    void clear(String workflowId, String servantId);
    boolean auth(String workflowId, String servantId, String token);

    void saveMeta(String workflowId, String servantId, String meta);

    @Nullable
    String getMeta(String workflowId, String servantId);
}
