package ai.lzy.scheduler.test.mocks;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
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
    public AllocatorMock(ServantDao dao) {
        super(dao);
    }

    @Override
    public AllocateResult allocate(String workflowId, String servantId, Provisioning provisioning, Env env) {
        var token = UUID.randomUUID().toString();
        onAllocate.forEach(t -> t.call(workflowId, servantId, token));
        saveRequest(workflowId, servantId, token, "");
        return new AllocateResult(token, "");
    }

    @Override
    public void destroy(String workflowId, String servantId) throws Exception {
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
