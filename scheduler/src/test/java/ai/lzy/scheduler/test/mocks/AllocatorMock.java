package ai.lzy.scheduler.test.mocks;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.allocator.ServantMetaStorage;
import ai.lzy.scheduler.allocator.ServantsAllocatorBase;
import ai.lzy.scheduler.db.ServantDao;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

@Singleton
public class AllocatorMock extends ServantsAllocatorBase {
    private final List<OnAllocatedRequest> onAllocate = new ArrayList<>();
    private final List<BiConsumer<String, String>> onDestroy = new ArrayList<>();

    @Inject
    public AllocatorMock(ServantDao dao, ServantMetaStorage metaStorage) {
        super(dao, metaStorage);
    }

    @Override
    public void allocate(String workflowId, String servantId, Provisioning provisioning) {
        onAllocate.forEach(t -> t.call(workflowId, servantId, ""));
    }

    @Override
    public void destroy(String workflowId, String servantId) {
        metaStorage.clear(workflowId, servantId);
        onDestroy.forEach(t -> t.accept(workflowId, servantId));
    }

    public synchronized void onAllocationRequested(OnAllocatedRequest callback) {
        onAllocate.add(callback);
    }

    public synchronized void onDestroyRequested(BiConsumer<String, String> callback) {
        onDestroy.add(callback);
    }

    public interface OnAllocatedRequest {
        void call(String workflowId, String servantId, String token);
    }
}
