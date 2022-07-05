package ai.lzy.scheduler.test.mocks;

import ai.lzy.scheduler.db.ServantMetaStorageDao;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class MetaStorageMock implements ServantMetaStorageDao {
    private final Map<ServantKey, MetaStorageEntry> storage = new ConcurrentHashMap<>();

    @Override
    public void save(MetaStorageEntry entry) {
        storage.put(new ServantKey(entry.workflowId(), entry.servantId()), entry);
    }

    @Nullable
    @Override
    public MetaStorageEntry get(String workflowId, String servantId) {
        return storage.get(new ServantKey(workflowId, servantId));
    }

    @Override
    public void remove(String workflowId, String servantId) {
        storage.remove(new ServantKey(workflowId, servantId));
    }

    private record ServantKey(String workflowId, String servantId) {}
}
