package ai.lzy.scheduler.test.mocks;

import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import io.micronaut.context.annotation.Primary;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import javax.inject.Singleton;

@Singleton
@Primary // for tests only
public class AllocatorMock implements WorkersAllocator {
    private final List<OnAllocatedRequest> onAllocate = new ArrayList<>();
    private final List<BiConsumer<String, String>> onDestroy = new ArrayList<>();

    @Override
    public void allocate(String userId, String workflowId, String workerId, Operation.Requirements provisioning) {
        onAllocate.forEach(t -> t.call(workflowId, workerId, ""));
    }

    @Override
    public void free(String workflowId, String workerId) {
        onDestroy.forEach(t -> t.accept(workflowId, workerId));
    }

    public synchronized void onAllocationRequested(OnAllocatedRequest callback) {
        onAllocate.add(callback);
    }

    public synchronized void onDestroyRequested(BiConsumer<String, String> callback) {
        onDestroy.add(callback);
    }

    public interface OnAllocatedRequest {
        void call(String workflowId, String workerId, String token);
    }
}
