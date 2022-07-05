package ai.lzy.scheduler.db;

import javax.annotation.Nullable;

public interface ServantMetaStorageDao {
    void save(MetaStorageEntry entry);

    @Nullable
    MetaStorageEntry get(String workflowId, String servantId);

    void remove(String workflowId, String servantId);

    record MetaStorageEntry(
        String workflowId, String servantId,

        @Nullable String meta,
        @Nullable String token) {}
}
